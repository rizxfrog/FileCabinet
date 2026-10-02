package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"FileCabinet/internal/config"
	"FileCabinet/internal/modelscope"
	"FileCabinet/internal/store"
)

var ErrInvalid = errors.New("参数无效")
var ErrBusy = errors.New("文件操作结果待确认，暂不可使用；请联系管理员")

type Upstream interface {
	Validate(context.Context, string, int64) (string, error)
	Put(context.Context, string, string, io.Reader, int64) error
	Commit(context.Context, string, string, string, int64, []byte, []byte) error
	Delete(context.Context, string, string) error
	ReadFile(context.Context, string) ([]byte, error)
	Download(context.Context, string) (*http.Response, error)
	DownloadTarget(context.Context, string, string, string) (string, *http.Response, error)
}
type Service struct {
	C         config.Config
	S         *store.Store
	U         Upstream
	Now       func() time.Time
	Generate  func(bool) string
	locks     [256]chan struct{}
	Uncertain atomic.Bool
}

func New(c config.Config, s *store.Store, u Upstream) *Service {
	a := &Service{C: c, S: s, U: u, Now: time.Now, Generate: randomCode}
	for i := range a.locks {
		a.locks[i] = make(chan struct{}, 1)
	}
	return a
}
func (a *Service) lock(ctx context.Context, h string) (func(), error) {
	idx := sha256.Sum256([]byte(h))
	c := a.locks[int(idx[0])]
	select {
	case c <- struct{}{}:
		return func() { <-c }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func HashValid(h string) bool           { return regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(h) }
func (a *Service) Path(h string) string { return a.C.Namespace + "/" + h[:2] + "/" + h[2:] }

// MetaPath is the repository-committed metadata document for a hash. It sits beside
// the blob under the same namespace, so a shared repository never mixes namespaces and
// cleanup can address the pair symmetrically.
func (a *Service) MetaPath(h string) string {
	return a.C.Namespace + "/meta/" + h[:2] + "/" + h[2:] + ".json"
}

// Meta document kinds. A document without a recognised kind is not a FileCabinet
// record, which keeps an unrelated repository file at the same path from being adopted.
const (
	kindFile = "file"
	kindList = "file-list"
	// listLimit bounds a file list. The index is one commit and every entry costs a
	// metadata read on lookup, so an unbounded list would let one document fan out into
	// arbitrarily many upstream requests.
	listLimit = 1000
)

// metaDoc is the committed metadata document for both kinds. File-list entries are
// hashes only: names and sizes are read from each entry's own document, so a list never
// duplicates data that can drift from its source.
type metaDoc struct {
	Type     string   `json:"type"`
	Name     string   `json:"name"`
	Size     int64    `json:"size"`
	FileList []string `json:"file-list,omitempty"`
}

// metaFor encodes the document written alongside an uploaded blob. Only a name that
// passed ValidateFile is stored; a name equal to the hash means "no name was supplied".
func metaFor(f store.File) []byte {
	m := metaDoc{Type: kindFile, Size: f.Size}
	if f.Name != "" && f.Name != f.Hash {
		m.Name = f.Name
	}
	b, e := json.Marshal(m)
	if e != nil {
		return nil
	}
	return b
}

// listFor encodes the document for a file list. Size is the total of the entries the
// caller resolved; the list itself stores no bytes.
func listFor(name string, size int64, hashes []string) []byte {
	m := metaDoc{Type: kindList, Name: name, Size: size, FileList: hashes}
	b, e := json.Marshal(m)
	if e != nil {
		return nil
	}
	return b
}

// parseMeta decodes a committed document into a record. ok is false when the bytes are
// not a FileCabinet document, or when a list violates the invariants a writer must
// uphold (non-positive total size, or non-hash entries): a corrupt index is treated as
// absent rather than surfaced as a half-usable record.
func parseMeta(b []byte) (f store.File, hashes []string, ok bool) {
	var m metaDoc
	if json.Unmarshal(b, &m) != nil || m.Size < 0 {
		return store.File{}, nil, false
	}
	switch m.Type {
	case kindFile:
		if !validName(m.Name) {
			m.Name = ""
		}
		return store.File{Name: m.Name, Size: m.Size}, nil, true
	case kindList:
		if len(m.FileList) == 0 || len(m.FileList) > listLimit || m.Size <= 0 || !validName(m.Name) {
			return store.File{}, nil, false
		}
		for _, h := range m.FileList {
			if !HashValid(h) {
				return store.File{}, nil, false
			}
		}
		return store.File{Name: m.Name, Size: m.Size}, m.FileList, true
	}
	return store.File{}, nil, false
}

func validName(name string) bool {
	return name != "" && len(name) <= 255 && utf8.ValidString(name) && !strings.ContainsAny(name, "\r\n\x00/\\")
}

// readMeta returns the document for a hash, or ok=false when it is absent or unusable.
func (a *Service) readMeta(ctx context.Context, h string) (store.File, []string, bool) {
	b, e := a.U.ReadFile(ctx, a.MetaPath(h))
	if e != nil || len(b) == 0 {
		return store.File{}, nil, false
	}
	return parseMeta(b)
}
func active(f store.File, now int64) bool { return f.ExpiresAt == 0 || f.ExpiresAt > now }
func MergeExpiry(old, new int64) int64 {
	if old == 0 || new == 0 {
		return 0
	}
	if old > new {
		return old
	}
	return new
}
func CodeExpiry(file, requested, now int64) (int64, error) {
	max := now + 7*86400
	if file != 0 && file < max {
		max = file
	}
	if max <= now {
		return 0, store.ErrMissing
	}
	if requested == 0 {
		return max, nil
	}
	if requested <= now || requested >= max {
		return 0, ErrInvalid
	}
	return requested, nil
}
func (a *Service) ValidateFile(f store.File) error {
	if !HashValid(f.Hash) || f.Size < 0 || f.Size > a.C.MaxBytes || len(f.Name) > 255 || !utf8.ValidString(f.Name) || strings.ContainsAny(f.Name, "\r\n\x00/\\") || (f.ExpiresAt != 0 && (f.ExpiresAt <= a.Now().Unix() || f.ExpiresAt > 253402300799)) {
		return ErrInvalid
	}
	return nil
}
func (a *Service) ready(ctx context.Context, h string) error {
	if e := a.S.Check(ctx); e != nil {
		return e
	}
	p, e := a.S.Pending(ctx, h)
	if e != nil {
		return e
	}
	if p != "" {
		return ErrBusy
	}
	return nil
}

// All same-hash metadata mutations and upstream operations remain under this lock.
func (a *Service) Upload(ctx context.Context, f store.File, r io.Reader) (store.File, error) {
	if e := a.ValidateFile(f); e != nil {
		return f, e
	}
	unlock, e := a.lock(ctx, f.Hash)
	if e != nil {
		return f, e
	}
	defer unlock()
	if e = a.ready(ctx, f.Hash); e != nil {
		return f, e
	}
	old, e := a.S.Get(ctx, f.Hash)
	if e != nil && !errors.Is(e, store.ErrMissing) {
		return f, e
	}
	exists := e == nil
	if exists && !active(old, a.Now().Unix()) {
		if e = a.deleteLocked(ctx, old); e != nil {
			return f, e
		}
		exists = false
	}
	// Even a deduplicated proxy request is fully hashed; untrusted metadata cannot bypass verification.
	digest := sha256.New()
	counted := &counter{r: io.TeeReader(io.LimitReader(r, f.Size+1), digest)}
	var content []byte
	if f.Size <= modelscope.InlineLimit {
		content, e = io.ReadAll(counted)
	} else if !exists {
		var target string
		target, e = a.U.Validate(ctx, f.Hash, f.Size)
		if e == nil {
			if target == "" {
				_, e = io.Copy(io.Discard, counted)
			} else {
				e = a.U.Put(ctx, target, f.Hash, counted, f.Size)
			}
		}
	} else {
		_, e = io.Copy(io.Discard, counted)
	}
	if e != nil {
		return f, e
	}
	// Transport may consume exactly Content-Length; explicitly check for an extra byte as well.
	_, e = io.Copy(io.Discard, counted)
	if e != nil {
		return f, e
	}
	if counted.n != f.Size || hex.EncodeToString(digest.Sum(nil)) != f.Hash {
		return f, fmt.Errorf("%w：文件大小或 SHA-256 不匹配", ErrInvalid)
	}
	if !active(f, a.Now().Unix()) {
		return f, fmt.Errorf("%w：上传期间有效期已结束", ErrInvalid)
	}
	if exists {
		if old.Size != f.Size {
			return f, ErrInvalid
		}
		old.ExpiresAt = MergeExpiry(old.ExpiresAt, f.ExpiresAt)
		e = a.S.Save(ctx, old)
		if e != nil {
			a.Uncertain.Store(true)
		}
		return a.uploadResult(old, e)
	}
	return a.commitLocked(ctx, f, content)
}

type counter struct {
	r io.Reader
	n int64
}

func (c *counter) Read(p []byte) (int, error) { n, e := c.r.Read(p); c.n += int64(n); return n, e }

// The stored file/index remains available to cleanup even if its deadline passed
// during a successful remote commit or Redis save.
func (a *Service) uploadResult(f store.File, err error) (store.File, error) {
	if err == nil && !active(f, a.Now().Unix()) {
		err = fmt.Errorf("%w：上传期间有效期已结束", ErrInvalid)
	}
	return f, err
}
func (a *Service) commitLocked(ctx context.Context, f store.File, content []byte) (store.File, error) {
	f.CreatedAt = a.Now().Unix()
	if f.Name == "" {
		f.Name = f.Hash
	}
	if e := a.S.Mark(ctx, f.Hash, "commit"); e != nil {
		a.Uncertain.Store(true)
		return f, e
	}
	if e := a.U.Commit(ctx, a.Path(f.Hash), a.MetaPath(f.Hash), f.Hash, f.Size, content, metaFor(f)); e != nil {
		return f, e
	}
	if e := a.S.Save(ctx, f); e != nil {
		a.Uncertain.Store(true)
		return f, e
	}
	return a.uploadResult(f, nil)
}
func (a *Service) deleteLocked(ctx context.Context, f store.File) error {
	if e := a.S.Mark(ctx, f.Hash, "delete-inflight"); e != nil {
		a.Uncertain.Store(true)
		return e
	}
	if e := a.U.Delete(ctx, a.Path(f.Hash), a.MetaPath(f.Hash)); e != nil {
		state := "delete"
		if errors.Is(e, modelscope.ErrUncertainMutation) || errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled) {
			state = "delete-uncertain"
			a.Uncertain.Store(true)
		}
		journalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if markErr := a.S.Mark(journalCtx, f.Hash, state); markErr != nil {
			a.Uncertain.Store(true)
		}
		return e
	}
	if e := a.S.Remove(ctx, f.Hash); e != nil {
		a.Uncertain.Store(true)
		return e
	}
	return nil
}
func (a *Service) Resolve(ctx context.Context, key string) (store.File, error) {
	h := key
	var codeExpiry int64
	if !HashValid(h) {
		if !regexp.MustCompile(`^[A-Za-z0-9]{6}$`).MatchString(key) {
			return store.File{}, ErrInvalid
		}
		c, e := a.S.Code(ctx, key)
		if e != nil {
			return store.File{}, e
		}
		if c.ExpiresAt <= a.Now().Unix() {
			return store.File{}, store.ErrMissing
		}
		h = c.Hash
		codeExpiry = c.ExpiresAt
	}
	unlock, e := a.lock(ctx, h)
	if e != nil {
		return store.File{}, e
	}
	defer unlock()
	if codeExpiry != 0 && codeExpiry <= a.Now().Unix() {
		return store.File{}, store.ErrMissing
	}
	if e = a.ready(ctx, h); e != nil {
		return store.File{}, e
	}
	f, e := a.S.Get(ctx, h)
	if e != nil {
		if !errors.Is(e, store.ErrMissing) {
			return store.File{}, e
		}
		return a.recover(ctx, h)
	}
	if !active(f, a.Now().Unix()) {
		return store.File{}, store.ErrMissing
	}
	// The committed document is the durable source of the name; Redis carries the
	// authoritative lifetime. A failing read must not make the entry unavailable.
	if doc, _, ok := a.readMeta(ctx, h); ok && doc.Name != "" {
		f.Name = doc.Name
	}
	return f, nil
}

// recover reconstructs a record when Redis has no entry for the hash, using the
// committed metadata document. It is limited to entries this service committed: the
// document must carry a recognised kind, which keeps an unrelated repository file at the
// same path from being adopted. Whether a blob still exists is decided by the caller's
// subsequent download, so a dangling document cannot fabricate an available file.
//
// The returned record has no expiry: Redis held the only copy of that deadline, so the
// entry is treated as permanent until the upload is repeated.
func (a *Service) recover(ctx context.Context, h string) (store.File, error) {
	doc, _, ok := a.readMeta(ctx, h)
	if !ok || doc.Size > a.C.MaxBytes {
		return store.File{}, store.ErrMissing
	}
	name := doc.Name
	if name == "" {
		name = h
	}
	return store.File{Hash: h, Name: name, Size: doc.Size}, nil
}

// ListEntry is one member of a file list. Entries carry the name and size read from the
// entry's own document, so the index never duplicates data that can drift.
type ListEntry struct {
	Hash string `json:"hash"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// CreateList commits an index document that points at existing files.
//
// The list itself stores no bytes, so it cannot go through Upload: its hash is the
// SHA-256 of the document, not of a blob. Entry hashes must already exist, so a list can
// only ever reference content that is genuinely stored — this deliberately does not
// verify reachability beyond the committed document, which would cost one read per entry.
func (a *Service) CreateList(ctx context.Context, name string, hashes []string) (store.File, error) {
	if !validName(name) {
		return store.File{}, fmt.Errorf("%w：列表名称无效", ErrInvalid)
	}
	if len(hashes) == 0 {
		return store.File{}, fmt.Errorf("%w：文件列表不能为空", ErrInvalid)
	}
	if len(hashes) > listLimit {
		return store.File{}, fmt.Errorf("%w：文件列表最多 %d 项", ErrInvalid, listLimit)
	}
	seen := make(map[string]bool, len(hashes))
	var total int64
	// Every entry must resolve to a plain file. Rejecting a nested list keeps indexing
	// acyclic, and rejecting duplicates keeps the total a sum of distinct content.
	for _, h := range hashes {
		if !HashValid(h) {
			return store.File{}, fmt.Errorf("%w：文件列表包含无效 hash", ErrInvalid)
		}
		if seen[h] {
			return store.File{}, fmt.Errorf("%w：文件列表包含重复项", ErrInvalid)
		}
		seen[h] = true
		doc, nested, ok := a.readMeta(ctx, h)
		if !ok {
			return store.File{}, fmt.Errorf("%w：文件列表包含不存在的文件", ErrInvalid)
		}
		if len(nested) > 0 {
			return store.File{}, fmt.Errorf("%w：文件列表不能包含文件列表", ErrInvalid)
		}
		total += doc.Size
	}
	doc := listFor(name, total, hashes)
	sum := sha256.Sum256(doc)
	hash := hex.EncodeToString(sum[:])
	unlock, e := a.lock(ctx, hash)
	if e != nil {
		return store.File{}, e
	}
	defer unlock()
	if e = a.ready(ctx, hash); e != nil {
		return store.File{}, e
	}
	// A list hash is derived from its content, so re-creating an identical list is a
	// no-op. The document is still written when only Redis lacks the record, which keeps
	// the fix correct after a lost database.
	existing, err := a.S.Get(ctx, hash)
	if err == nil && active(existing, a.Now().Unix()) && existing.Size == total {
		return existing, nil
	}
	if err != nil && !errors.Is(err, store.ErrMissing) {
		return store.File{}, err
	}
	if e = a.S.Mark(ctx, hash, "commit"); e != nil {
		a.Uncertain.Store(true)
		return store.File{}, e
	}
	if e = a.U.Commit(ctx, "", a.MetaPath(hash), hash, total, nil, doc); e != nil {
		return store.File{}, e
	}
	f := store.File{Hash: hash, Name: name, Size: total, CreatedAt: a.Now().Unix()}
	if e = a.S.Save(ctx, f); e != nil {
		a.Uncertain.Store(true)
		return f, e
	}
	return f, nil
}

// Describe expands a key into either a file or an index whose entries carry their own
// names and sizes. Entry metadata is read concurrently and bounded; a single unusable
// entry fails the whole description rather than reporting a partial list. The returned
// keys are the entry hashes in document order, for callers that download them.
func (a *Service) Describe(ctx context.Context, key string) (store.File, []ListEntry, error) {
	f, e := a.Resolve(ctx, key)
	if e != nil {
		return f, nil, e
	}
	_, hashes, ok := a.readMeta(ctx, f.Hash)
	if !ok || len(hashes) == 0 {
		return f, nil, nil
	}
	entries := make([]ListEntry, len(hashes))
	limit := make(chan struct{}, 8)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	for i, h := range hashes {
		wg.Add(1)
		limit <- struct{}{}
		go func(i int, h string) {
			defer wg.Done()
			defer func() { <-limit }()
			doc, _, ok := a.readMeta(ctx, h)
			if !ok {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("%w：文件列表项不可用", ErrInvalid)
				}
				mu.Unlock()
				return
			}
			name := doc.Name
			if name == "" {
				name = h
			}
			entries[i] = ListEntry{Hash: h, Name: name, Size: doc.Size}
		}(i, h)
	}
	wg.Wait()
	if firstErr != nil {
		return f, nil, firstErr
	}
	return f, entries, nil
}

func (a *Service) NewCode(ctx context.Context, h string, expiry int64) (store.Code, error) {
	var c store.Code
	if !HashValid(h) {
		return c, ErrInvalid
	}
	unlock, e := a.lock(ctx, h)
	if e != nil {
		return c, e
	}
	defer unlock()
	if e = a.ready(ctx, h); e != nil {
		return c, e
	}
	f, e := a.S.Get(ctx, h)
	if e != nil {
		return c, e
	}
	now := a.Now().Unix()
	expiry, e = CodeExpiry(f.ExpiresAt, expiry, now)
	if e != nil {
		return c, e
	}
	c = store.Code{Hash: h, ExpiresAt: expiry}
	for i := 0; i < 32; i++ {
		c.Code = a.Generate(i > 0)
		ok, e := a.S.AddCode(ctx, c, now)
		if e != nil {
			return c, e
		}
		if ok {
			return c, nil
		}
	}
	return c, errors.New("短码空间繁忙，请重试")
}
func randomCode(mixed bool) string {
	chars := "0123456789"
	if mixed {
		chars += "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	}
	b := make([]byte, 6)
	for i := range b {
		n, e := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		if e != nil {
			panic(e)
		}
		b[i] = chars[n.Int64()]
	}
	if mixed {
		hasD, hasL := false, false
		for _, c := range b {
			if c >= '0' && c <= '9' {
				hasD = true
			} else {
				hasL = true
			}
		}
		if !hasD || !hasL {
			return randomCode(true)
		}
	}
	return string(b)
}
func (a *Service) Cleanup(ctx context.Context) error {
	if e := a.S.Check(ctx); e != nil {
		return e
	}
	hs, e := a.S.Due(ctx, a.Now().Unix())
	if e != nil {
		return e
	}
	var first error
	for _, h := range hs {
		if !HashValid(h) {
			continue
		}
		func() {
			unlock, e := a.lock(ctx, h)
			if e != nil {
				first = e
				return
			}
			defer unlock()
			if e = a.S.Check(ctx); e != nil {
				first = e
				return
			}
			f, e := a.S.Get(ctx, h)
			if errors.Is(e, store.ErrMissing) {
				first = a.S.Remove(ctx, h)
				return
			}
			if e != nil {
				first = e
				return
			}
			if active(f, a.Now().Unix()) {
				return
			}
			p, e := a.S.Pending(ctx, h)
			if e != nil {
				first = e
				return
			}
			if p != "" && p != "delete" {
				// Quarantined entries must not monopolize the first cleanup batch.
				if err := a.S.Retry(ctx, h, a.Now().Unix()+3600); err != nil {
					first = err
				}
				return
			}
			if e = a.deleteLocked(ctx, f); e != nil {
				// The flag is set before any journal write so an unreachable Redis cannot
				// crash the loop with a sticky bit that never got published: main decides
				// whether to keep the owner based on it.
				a.Uncertain.Store(true)
				first = e
				retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				if e = a.S.Retry(retryCtx, h, a.Now().Unix()+60); e != nil {
					slog.Warn("cleanup retry not rescheduled", "hash", h)
				}
			}
		}()
	}
	return first
}
func (a *Service) Prepare(ctx context.Context, f store.File) (map[string]any, error) {
	if a.C.UploadMode != "direct" || f.Size <= modelscope.InlineLimit {
		return nil, ErrInvalid
	}
	if e := a.ValidateFile(f); e != nil {
		return nil, e
	}
	unlock, e := a.lock(ctx, f.Hash)
	if e != nil {
		return nil, e
	}
	defer unlock()
	if e = a.ready(ctx, f.Hash); e != nil {
		return nil, e
	}
	target, e := a.U.Validate(ctx, f.Hash, f.Size)
	if e != nil {
		return nil, e
	}
	id := store.Random()
	if e = a.S.PutSession(ctx, id, f); e != nil {
		return nil, e
	}
	return map[string]any{"session_id": id, "upload_url": target, "method": "PUT", "expires_in": 3600}, nil
}
func (a *Service) Complete(ctx context.Context, id string) (store.File, error) {
	var f store.File
	if !regexp.MustCompile(`^[a-f0-9]{48}$`).MatchString(id) {
		return f, ErrInvalid
	}
	f, e := a.S.Session(ctx, id)
	if e != nil {
		return f, e
	}
	if e = a.ValidateFile(f); e != nil {
		return f, e
	}
	unlock, e := a.lock(ctx, f.Hash)
	if e != nil {
		return f, e
	}
	defer unlock()
	if e = a.ready(ctx, f.Hash); e != nil {
		return f, e
	}
	target, e := a.U.Validate(ctx, f.Hash, f.Size)
	if e != nil {
		return f, e
	}
	if target != "" {
		return f, fmt.Errorf("%w：远端 blob 尚未上传完成", ErrInvalid)
	}
	if !active(f, a.Now().Unix()) {
		return f, fmt.Errorf("%w：上传会话指定的文件有效期已结束", ErrInvalid)
	}
	old, e := a.S.Get(ctx, f.Hash)
	if e != nil && !errors.Is(e, store.ErrMissing) {
		return f, e
	}
	if e == nil {
		if !active(old, a.Now().Unix()) {
			if e = a.deleteLocked(ctx, old); e != nil {
				return f, e
			}
		} else {
			if old.Size != f.Size {
				return f, ErrInvalid
			}
			old.ExpiresAt = MergeExpiry(old.ExpiresAt, f.ExpiresAt)
			e = a.S.Save(ctx, old)
			if e != nil {
				a.Uncertain.Store(true)
			}
			return a.uploadResult(old, e)
		}
	}
	f, e = a.commitLocked(ctx, f, nil)
	if e == nil {
		_ = a.S.EndSession(ctx, id)
	}
	return f, e
}

// Hold the file lock until the response body is closed so cleanup cannot delete during download.
func (a *Service) Open(ctx context.Context, key string) (store.File, *http.Response, error) {
	f, unlock, e := a.authorize(ctx, key)
	if e != nil {
		return f, nil, e
	}
	resp, e := a.U.Download(ctx, a.Path(f.Hash))
	if e != nil {
		unlock()
		return f, nil, e
	}
	resp.Body = &lockedBody{ReadCloser: resp.Body, unlock: unlock}
	return f, resp, nil
}

// DirectDownload authorizes a download exactly like Open, then asks upstream for a
// URL the browser can fetch itself. It returns (signedURL, nil) when upstream issued
// a redirect, or ("", resp) when upstream streamed the body directly (inline and other
// non-LFS files); the caller then proxies resp like Open, without a second request.
//
// The per-hash lock is held only for the request itself. No lock protects the bytes
// after a redirect: an already-issued URL cannot be revoked, so expiry, short codes
// and cleanup still govern future requests but not the URL just handed out. Callers
// must treat a signed URL as a bearer credential and must not log it.
func (a *Service) DirectDownload(ctx context.Context, key string) (store.File, string, *http.Response, error) {
	f, unlock, e := a.authorize(ctx, key)
	if e != nil {
		return f, "", nil, e
	}
	loc, resp, e := a.U.DownloadTarget(ctx, a.Path(f.Hash), f.Hash, f.Name)
	if e != nil {
		unlock()
		return f, "", nil, e
	}
	if resp != nil {
		resp.Body = &lockedBody{ReadCloser: resp.Body, unlock: unlock}
		return f, "", resp, nil
	}
	// No bytes flow through this process, so the lock can be released now.
	unlock()
	return f, loc, nil, nil
}

// authorize performs the full read authorization: resolve key or short code, take
// the per-hash lock, verify the owner claim, the pending journal, the file expiry
// and the short-code validity. The caller owns the returned unlock function.
func (a *Service) authorize(ctx context.Context, key string) (store.File, func(), error) {
	resolved, e := a.Resolve(ctx, key)
	if e != nil {
		return resolved, nil, e
	}
	unlock, e := a.lock(ctx, resolved.Hash)
	if e != nil {
		return resolved, nil, e
	}
	if e = a.ready(ctx, resolved.Hash); e != nil {
		unlock()
		return resolved, nil, e
	}
	// Re-read under the lock: the lock serializes against cleanup, so a missing record
	// here means none exists rather than one being removed. The record Resolve produced
	// stays valid, which is what keeps a meta-recovered file downloadable.
	f, e := a.S.Get(ctx, resolved.Hash)
	if errors.Is(e, store.ErrMissing) {
		f, e = resolved, nil
	}
	if e != nil || !active(f, a.Now().Unix()) {
		unlock()
		if e == nil {
			e = store.ErrMissing
		}
		return f, nil, e
	}
	if !HashValid(key) {
		code, err := a.S.Code(ctx, key)
		if err != nil || code.Hash != f.Hash || code.ExpiresAt <= a.Now().Unix() {
			unlock()
			if err == nil {
				err = store.ErrMissing
			}
			return f, nil, err
		}
	}
	return f, unlock, nil
}

type lockedBody struct {
	io.ReadCloser
	unlock func()
}

func (b *lockedBody) Close() error { e := b.ReadCloser.Close(); b.unlock(); return e }

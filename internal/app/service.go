package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"regexp"
	"strings"
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
	Commit(context.Context, string, string, int64, []byte) error
	Delete(context.Context, string) error
	Download(context.Context, string) (*http.Response, error)
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
func HashValid(h string) bool             { return regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(h) }
func (a *Service) Path(h string) string   { return a.C.Namespace + "/" + h[:2] + "/" + h[2:] }
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
	if e := a.U.Commit(ctx, a.Path(f.Hash), f.Hash, f.Size, content); e != nil {
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
	if e := a.U.Delete(ctx, a.Path(f.Hash)); e != nil {
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
	if e == nil && !active(f, a.Now().Unix()) {
		e = store.ErrMissing
	}
	return f, e
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
				first = e
				retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				if e = a.S.Retry(retryCtx, h, a.Now().Unix()+60); e != nil {
					a.Uncertain.Store(true)
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
	f, e := a.Resolve(ctx, key)
	if e != nil {
		return f, nil, e
	}
	unlock, e := a.lock(ctx, f.Hash)
	if e != nil {
		return f, nil, e
	}
	if e = a.ready(ctx, f.Hash); e != nil {
		unlock()
		return f, nil, e
	}
	f, e = a.S.Get(ctx, f.Hash)
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
	resp, e := a.U.Download(ctx, a.Path(f.Hash))
	if e != nil {
		unlock()
		return f, nil, e
	}
	resp.Body = &lockedBody{ReadCloser: resp.Body, unlock: unlock}
	return f, resp, nil
}

type lockedBody struct {
	io.ReadCloser
	unlock func()
}

func (b *lockedBody) Close() error { e := b.ReadCloser.Close(); b.unlock(); return e }

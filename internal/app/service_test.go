package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"FileCabinet/internal/config"
	"FileCabinet/internal/modelscope"
	"FileCabinet/internal/store"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeUpstream struct {
	afterCommit                     func()
	mu                              sync.Mutex
	commits, deletes, puts, reads   int
	failCommit, failDelete, failPut bool
	failRead                        bool
	uncertainDelete                 bool
	validateURL                     string
	directLoc                       string
	deleteEntered, deleteRelease    chan struct{}
	content                         []byte
	meta                            []byte
	metaPath                        string
	files                           map[string][]byte
}

func (u *fakeUpstream) Validate(context.Context, string, int64) (string, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.validateURL, nil
}
func (u *fakeUpstream) Put(_ context.Context, _, _ string, r io.Reader, _ int64) error {
	u.mu.Lock()
	u.puts++
	bad := u.failPut
	u.mu.Unlock()
	if bad {
		return modelscope.ErrUpstream
	}
	_, e := io.Copy(io.Discard, r)
	return e
}
func (u *fakeUpstream) Commit(_ context.Context, _, metaPath, _ string, _ int64, b, meta []byte) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.commits++
	u.content = append([]byte(nil), b...)
	u.metaPath = metaPath
	u.meta = append([]byte(nil), meta...)
	if u.files != nil && metaPath != "" {
		u.files[metaPath] = append([]byte(nil), meta...)
	}
	if u.afterCommit != nil {
		u.afterCommit()
	}
	if u.failCommit {
		return modelscope.ErrUpstream
	}
	return nil
}
func (u *fakeUpstream) Delete(_ context.Context, _, metaPath string) error {
	if u.deleteEntered != nil {
		u.deleteEntered <- struct{}{}
		<-u.deleteRelease
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.deletes++
	if u.files != nil && metaPath != "" {
		delete(u.files, metaPath)
	}
	if u.uncertainDelete {
		return modelscope.ErrUncertainMutation
	}
	if u.failDelete {
		return modelscope.ErrUpstream
	}
	return nil
}

// ReadFile serves a metadata document. A nil byte slice means the path does not exist,
// matching the real client's absent-path contract.
func (u *fakeUpstream) ReadFile(_ context.Context, path string) ([]byte, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.reads++
	if u.failRead {
		return nil, modelscope.ErrUpstream
	}
	if u.files == nil {
		return nil, nil
	}
	return append([]byte(nil), u.files[path]...), nil
}
func (u *fakeUpstream) Download(context.Context, string) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(u.content))}, nil
}

// directLoc is returned as a signed redirect when set; otherwise a 200 body is
// returned so the caller exercises the inline/proxy fallback without a second call.
func (u *fakeUpstream) DownloadTarget(context.Context, string, string, string) (string, *http.Response, error) {
	u.mu.Lock()
	direct := u.directLoc
	u.mu.Unlock()
	if direct != "" {
		return direct, nil, nil
	}
	return "", &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(u.content))}, nil
}
func setup(t *testing.T) (*Service, *fakeUpstream, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { r.Close() })
	s := store.New(r, "test:")
	if e := s.Claim(context.Background()); e != nil {
		t.Fatal(e)
	}
	c := config.Config{Namespace: "05", UploadMode: "proxy", DownloadMode: "proxy", MaxBytes: 20 << 20, Timeout: time.Second * 10}
	u := &fakeUpstream{}
	a := New(c, s, u)
	a.Now = func() time.Time { return time.Unix(1800000000, 0) }
	return a, u, mr
}
func file(data []byte, expiry int64) store.File {
	h := sha256.Sum256(data)
	return store.File{Hash: hex.EncodeToString(h[:]), Name: "说明.txt", Size: int64(len(data)), ExpiresAt: expiry}
}
func TestExpiryRules(t *testing.T) {
	now := int64(1000)
	for _, tt := range []struct{ old, new, want int64 }{{0, 2000, 0}, {2000, 0, 0}, {2000, 3000, 3000}, {3000, 2000, 3000}} {
		if got := MergeExpiry(tt.old, tt.new); got != tt.want {
			t.Fatalf("merge got %d", got)
		}
	}
	for _, tt := range []struct {
		file, custom, want int64
		bad                bool
	}{{0, 0, now + 604800, false}, {2000, 0, 2000, false}, {2000, 1999, 1999, false}, {2000, 2000, 0, true}, {1000, 0, 0, true}, {0, 1000, 0, true}, {0, now + 604800, 0, true}} {
		got, e := CodeExpiry(tt.file, tt.custom, now)
		if (e != nil) != tt.bad || got != tt.want {
			t.Fatalf("expiry %+v got %d %v", tt, got, e)
		}
	}
}
func TestProxyDuplicateAndHashVerification(t *testing.T) {
	a, u, _ := setup(t)
	ctx := context.Background()
	b := []byte("hello")
	f := file(b, a.Now().Unix()+3600)
	saved, e := a.Upload(ctx, f, bytes.NewReader(b))
	if e != nil {
		t.Fatal(e)
	}
	if a.Path(f.Hash) != "05/"+f.Hash[:2]+"/"+f.Hash[2:] {
		t.Fatal("path")
	}
	if !bytes.Equal(u.content, b) || saved.CreatedAt == 0 {
		t.Fatal("not committed")
	}
	f.ExpiresAt = a.Now().Unix() + 86400
	if _, e = a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	got, _ := a.S.Get(ctx, f.Hash)
	if got.ExpiresAt != f.ExpiresAt || u.commits != 1 {
		t.Fatal("duplicate did not extend")
	}
	f.ExpiresAt = 0
	if _, e = a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	f.ExpiresAt = a.Now().Unix() + 10
	if _, e = a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	got, _ = a.S.Get(ctx, f.Hash)
	if got.ExpiresAt != 0 {
		t.Fatal("permanent shortened")
	}
	if _, e = a.Upload(ctx, f, strings.NewReader("wrong")); !errors.Is(e, ErrInvalid) {
		t.Fatalf("expected hash reject: %v", e)
	}
	if u.commits != 1 {
		t.Fatal("bad commit")
	}
}
func TestUploadFailureNeverRegisters(t *testing.T) {
	for _, kind := range []string{"hash", "short", "long", "commit", "put"} {
		t.Run(kind, func(t *testing.T) {
			a, u, _ := setup(t)
			ctx := context.Background()
			b := []byte("hello")
			if kind == "put" {
				b = bytes.Repeat([]byte("a"), int(modelscope.InlineLimit)+1)
				u.validateURL = "https://test.aliyuncs.com/upload"
				u.failPut = true
			}
			f := file(b, 0)
			input := b
			switch kind {
			case "hash":
				input = []byte("wrong")
			case "short":
				input = b[:3]
			case "long":
				input = append(input, '!')
			case "commit":
				u.failCommit = true
			}
			_, e := a.Upload(ctx, f, bytes.NewReader(input))
			if e == nil {
				t.Fatal("expected failure")
			}
			if _, e = a.S.Get(ctx, f.Hash); !errors.Is(e, store.ErrMissing) {
				t.Fatal("registered failed upload")
			}
			if kind == "commit" {
				if e = a.S.Release(ctx); e == nil {
					t.Fatal("released uncertain owner")
				}
				if _, e = a.Upload(ctx, f, bytes.NewReader(b)); !errors.Is(e, ErrBusy) {
					t.Fatal("uncertain commit was retried")
				}
			} else if u.commits != 0 {
				t.Fatal("committed unverified bytes")
			}
		})
	}
}
func TestLargeStreamingReuseAndUpload(t *testing.T) {
	for _, target := range []string{"", "https://x.aliyuncs.com/put"} {
		t.Run(target, func(t *testing.T) {
			a, u, _ := setup(t)
			u.validateURL = target
			b := bytes.Repeat([]byte("b"), int(modelscope.InlineLimit)+123)
			f := file(b, 0)
			if _, e := a.Upload(context.Background(), f, bytes.NewReader(b)); e != nil {
				t.Fatal(e)
			}
			if len(u.content) != 0 || u.commits != 1 {
				t.Fatal("large content buffered or not committed")
			}
			if target != "" && u.puts != 1 {
				t.Fatal("not proxied")
			}
		})
	}
}
func TestCodeConflictTTLAndMissing(t *testing.T) {
	a, _, mr := setup(t)
	ctx := context.Background()
	b := []byte("code")
	f := file(b, a.Now().Unix()+3600)
	if _, e := a.NewCode(ctx, f.Hash, 0); !errors.Is(e, store.ErrMissing) {
		t.Fatal(e)
	}
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	a.Generate = func(mixed bool) string {
		if mixed {
			return "ab12CD"
		}
		return "000001"
	}
	c, e := a.NewCode(ctx, f.Hash, 0)
	if e != nil || c.Code != "000001" || c.ExpiresAt != f.ExpiresAt {
		t.Fatal(c, e)
	}
	c, e = a.NewCode(ctx, f.Hash, 0)
	if e != nil || c.Code != "ab12CD" {
		t.Fatal(c, e)
	}
	if _, e = a.Resolve(ctx, c.Code); e != nil {
		t.Fatal(e)
	}
	if _, e = a.NewCode(ctx, f.Hash, f.ExpiresAt); !errors.Is(e, ErrInvalid) {
		t.Fatal("equal custom expiry accepted")
	}
	mr.FastForward(time.Hour)
	if _, e = a.Resolve(ctx, c.Code); !errors.Is(e, store.ErrMissing) {
		t.Fatal("code TTL")
	}
	if _, e = a.S.Get(ctx, f.Hash); e != nil {
		t.Fatal("file metadata expired with TTL")
	}
}
func TestCleanupRetryAndNoEarlyDelete(t *testing.T) {
	a, u, _ := setup(t)
	ctx := context.Background()
	b := []byte("expire")
	f := file(b, a.Now().Unix()+1)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	if e := a.Cleanup(ctx); e != nil || u.deletes != 0 {
		t.Fatal("early cleanup")
	}
	now := a.Now().Unix() + 2
	a.Now = func() time.Time { return time.Unix(now, 0) }
	u.failDelete = true
	if e := a.Cleanup(ctx); e == nil {
		t.Fatal("cleanup failure missing")
	}
	if _, e := a.S.Get(ctx, f.Hash); e != nil {
		t.Fatal("metadata lost")
	}
	if _, e := a.NewCode(ctx, f.Hash, 0); !errors.Is(e, ErrBusy) {
		t.Fatal("pending deletion usable")
	}
	f.ExpiresAt = 0
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); !errors.Is(e, ErrBusy) {
		t.Fatal("renewal bypassed pending deletion")
	}
	u.failDelete = false
	now += 61
	if e := a.Cleanup(ctx); e != nil {
		t.Fatal(e)
	}
	if u.deletes != 2 {
		t.Fatal("not retried")
	}
	if _, e := a.S.Get(ctx, f.Hash); !errors.Is(e, store.ErrMissing) {
		t.Fatal("file retained")
	}
	if e := a.S.Release(ctx); e != nil {
		t.Fatal("resolved deletion retained owner", e)
	}
}
func TestRenewalSerializesWithDelete(t *testing.T) {
	a, u, _ := setup(t)
	ctx := context.Background()
	b := []byte("race")
	f := file(b, a.Now().Unix()+1)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	now := a.Now().Unix() + 2
	a.Now = func() time.Time { return time.Unix(now, 0) }
	u.deleteEntered = make(chan struct{}, 1)
	u.deleteRelease = make(chan struct{})
	clean := make(chan error, 1)
	go func() { clean <- a.Cleanup(ctx) }()
	<-u.deleteEntered
	f.ExpiresAt = 0
	up := make(chan error, 1)
	go func() { _, e := a.Upload(ctx, f, bytes.NewReader(b)); up <- e }()
	select {
	case <-up:
		t.Fatal("renewal raced past in-flight deletion")
	case <-time.After(30 * time.Millisecond):
	}
	close(u.deleteRelease)
	if e := <-clean; e != nil {
		t.Fatal(e)
	}
	if e := <-up; e != nil {
		t.Fatal(e)
	}
	got, e := a.Resolve(ctx, f.Hash)
	if e != nil || got.ExpiresAt != 0 || u.commits != 2 {
		t.Fatal("renewal lost", got, e)
	}
}
func TestDirectRequiresUploadedBlob(t *testing.T) {
	a, u, _ := setup(t)
	a.C.UploadMode = "direct"
	ctx := context.Background()
	b := bytes.Repeat([]byte("d"), int(modelscope.InlineLimit)+1)
	f := file(b, 0)
	u.validateURL = "https://x.aliyuncs.com/put"
	prepared, e := a.Prepare(ctx, f)
	if e != nil {
		t.Fatal(e)
	}
	id := prepared["session_id"].(string)
	if _, e = a.Complete(ctx, id); !errors.Is(e, ErrInvalid) {
		t.Fatal("unuploaded blob accepted")
	}
	u.validateURL = ""
	if _, e = a.Complete(ctx, id); e != nil {
		t.Fatal(e)
	}
	if u.commits != 1 {
		t.Fatal("not committed")
	}
}
func TestHTTPLifecycleSmoke(t *testing.T) {
	a, _, _ := setup(t)
	life := &Lifecycle{}
	server := httptest.NewServer(life.Wrap(a.Handler(fstest.MapFS{"index.html": {Data: []byte("<title>文件柜</title>")}})))
	defer server.Close()
	client := server.Client()
	b := []byte("smoke")
	f := file(b, 0)
	req, _ := http.NewRequest("PUT", server.URL+"/api/files/"+f.Hash+"?size=5&name=smoke.txt", bytes.NewReader(b))
	resp, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatal(resp.Status)
	}
	resp, e = client.Get(server.URL + "/api/files/" + f.Hash)
	if e != nil {
		t.Fatal(e)
	}
	var lookup map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&lookup)
	resp.Body.Close()
	if lookup["download_url"] != "/api/download/"+f.Hash {
		t.Fatal(lookup)
	}
	resp, e = client.Get(server.URL + "/api/download/" + f.Hash)
	if e != nil {
		t.Fatal(e)
	}
	download, e := io.ReadAll(resp.Body)
	resp.Body.Close()
	if e != nil || !bytes.Equal(download, b) || resp.Header.Get("Content-Disposition") == "" {
		t.Fatal("download smoke", e)
	}
	resp, e = client.Post(server.URL+"/api/codes", "application/json", strings.NewReader(`{"hash":"`+f.Hash+`"}`))
	if e != nil {
		t.Fatal(e)
	}
	if resp.StatusCode != 201 {
		t.Fatal(resp.Status)
	}
	resp.Body.Close()
	for _, path := range []string{"/", "/healthz", "/api/config"} {
		resp, e = client.Get(server.URL + path)
		if e != nil {
			t.Fatal(e)
		}
		if resp.StatusCode != 200 {
			t.Fatal(path, resp.Status)
		}
		resp.Body.Close()
	}
	req, _ = http.NewRequest("PUT", server.URL+"/api/files/"+f.Hash+"?size=5", bytes.NewReader(b))
	req.Header.Set("Origin", "https://evil.example")
	resp, e = client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatal("cross-origin accepted")
	}
	life.Stop()
	life.Wait()
	resp, e = client.Get(server.URL + "/healthz")
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatal("shutdown admission")
	}
	if e = a.S.Release(context.Background()); e != nil {
		t.Fatal(e)
	}
}
func TestLifecycleDrains(t *testing.T) {
	life := &Lifecycle{}
	entered := make(chan struct{})
	release := make(chan struct{})
	h := life.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release }))
	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	<-entered
	life.Stop()
	done := make(chan struct{})
	go func() { life.Wait(); close(done) }()
	select {
	case <-done:
		t.Fatal("did not drain")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-done
}
func TestRedisFailureFailsClosed(t *testing.T) {
	a, u, mr := setup(t)
	mr.SetError("redis down")
	b := []byte("bad")
	if _, e := a.Upload(context.Background(), file(b, 0), bytes.NewReader(b)); e == nil {
		t.Fatal("redis failure ignored")
	}
	if u.commits != 0 {
		t.Fatal("upstream mutated")
	}
}

func TestUncertainDeleteQuarantinesWithoutRetry(t *testing.T) {
	a, u, _ := setup(t)
	ctx := context.Background()
	b := []byte("uncertain")
	f := file(b, a.Now().Unix()+1)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	now := a.Now().Unix() + 2
	a.Now = func() time.Time { return time.Unix(now, 0) }
	u.uncertainDelete = true
	if e := a.Cleanup(ctx); !errors.Is(e, modelscope.ErrUncertainMutation) {
		t.Fatal(e)
	}
	p, e := a.S.Pending(ctx, f.Hash)
	if e != nil || p != "delete-uncertain" {
		t.Fatal(p, e)
	}
	u.uncertainDelete = false
	now += 100
	if e := a.Cleanup(ctx); e != nil {
		t.Fatal(e)
	}
	if u.deletes != 1 {
		t.Fatal("retried uncertain deletion")
	}
	if e := a.S.Release(ctx); e == nil {
		t.Fatal("released unknown delete owner")
	}
	f.ExpiresAt = 0
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); !errors.Is(e, ErrBusy) {
		t.Fatal("renewed over unknown delete")
	}
	if !a.Uncertain.Load() {
		t.Fatal("shutdown did not retain claim")
	}
}

func TestShortCodeExpiresWhileWaitingForLock(t *testing.T) {
	a, _, _ := setup(t)
	ctx := context.Background()
	b := []byte("code-lock")
	f := file(b, 0)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	c, e := a.NewCode(ctx, f.Hash, a.Now().Unix()+1)
	if e != nil {
		t.Fatal(e)
	}
	unlock, e := a.lock(ctx, f.Hash)
	if e != nil {
		t.Fatal(e)
	}
	// Deterministic clock advances on each lookup after the first read of code validity.
	var calls atomic.Int64
	now := a.Now().Unix()
	a.Now = func() time.Time {
		if calls.Add(1) == 1 {
			return time.Unix(now, 0)
		}
		return time.Unix(now+2, 0)
	}
	result := make(chan error, 1)
	go func() { _, err := a.Resolve(ctx, c.Code); result <- err }()
	unlock()
	if e := <-result; !errors.Is(e, store.ErrMissing) {
		t.Fatal("expired code passed lock", e)
	}
}

// Run a clock change after Redis has actually completed the file save transaction.
type afterSaveHook struct{ after func() }

func (h afterSaveHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h afterSaveHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h afterSaveHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if err == nil {
			for _, cmd := range cmds {
				if cmd.Name() == "set" && strings.Contains(fmt.Sprint(cmd.Args()[1]), "file:") {
					h.after()
					break
				}
			}
		}
		return err
	}
}
func TestUploadExpiryAfterCommitAndSave(t *testing.T) {
	for _, mode := range []string{"proxy-new", "proxy-reuse", "direct-new", "direct-reuse"} {
		for _, phase := range []string{"commit", "save"} {
			if strings.HasSuffix(mode, "reuse") && phase == "commit" {
				continue
			}
			t.Run(mode+"/"+phase, func(t *testing.T) {
				a, u, _ := setup(t)
				ctx := context.Background()
				now := a.Now().Unix()
				a.Now = func() time.Time { return time.Unix(now, 0) }
				b := []byte("deadline")
				if strings.HasPrefix(mode, "direct") {
					a.C.UploadMode = "direct"
					b = bytes.Repeat([]byte("x"), int(modelscope.InlineLimit)+1)
				}
				f := file(b, now+10)
				if strings.HasSuffix(mode, "reuse") {
					if err := a.S.Save(ctx, f); err != nil {
						t.Fatal(err)
					}
				}
				if phase == "commit" {
					u.afterCommit = func() { now = f.ExpiresAt }
				} else {
					a.S.R.AddHook(afterSaveHook{after: func() { now = f.ExpiresAt }})
				}
				var err error
				if a.C.UploadMode == "direct" {
					prepared, e := a.Prepare(ctx, f)
					if e != nil {
						t.Fatal(e)
					}
					_, err = a.Complete(ctx, prepared["session_id"].(string))
				} else {
					_, err = a.Upload(ctx, f, bytes.NewReader(b))
				}
				if !errors.Is(err, ErrInvalid) {
					t.Fatal("expired upload reported success", err)
				}
				saved, e := a.S.Get(ctx, f.Hash)
				if e != nil || saved.ExpiresAt != f.ExpiresAt {
					t.Fatal("cleanup metadata lost", saved, e)
				}
				due, e := a.S.Due(ctx, now)
				if e != nil || len(due) != 1 || due[0] != f.Hash {
					t.Fatal("expiry index lost", due, e)
				}
				if p, e := a.S.Pending(ctx, f.Hash); e != nil || p != "" {
					t.Fatal("confirmed commit wrongly quarantined", p, e)
				}
				if _, e := a.Resolve(ctx, f.Hash); !errors.Is(e, store.ErrMissing) {
					t.Fatal("expired file served", e)
				}
				if e := a.Cleanup(ctx); e != nil || u.deletes != 1 {
					t.Fatal("expired committed file not cleaned", e)
				}
			})
		}
	}
}

type apiTransport func(*http.Request) (*http.Response, error)

func (f apiTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestMalformedMutationKeepsJournal(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"Code":200,"Success":"true"}`, `{"Code":200,"Success":false}`, `{"Code":500,"Success":true}`} {
		for _, action := range []string{"commit", "delete"} {
			t.Run(action+body, func(t *testing.T) {
				a, _, _ := setup(t)
				ctx := context.Background()
				c := modelscope.New(config.Config{Endpoint: "https://www.modelscope.cn", Repo: "o/r", RepoType: "models", Revision: "master"})
				// Count mutations only: delete now probes existence with reads first, so
				// an all-request counter would no longer describe retry behaviour.
				calls := 0
				c.API.Transport = apiTransport(func(r *http.Request) (*http.Response, error) {
					if strings.Contains(r.URL.Path, "/commit/") {
						calls++
					}
					status := 200
					if strings.Contains(r.URL.Query().Get("FilePath"), "meta") {
						status = 404
					}
					return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
				})
				a.U = c
				b := []byte("uncertain envelope")
				f := file(b, a.Now().Unix()+1)
				if action == "commit" {
					if _, err := a.Upload(ctx, f, bytes.NewReader(b)); !errors.Is(err, modelscope.ErrUncertainMutation) {
						t.Fatal(err)
					}
				} else {
					if err := a.S.Save(ctx, f); err != nil {
						t.Fatal(err)
					}
					a.Now = func() time.Time { return time.Unix(f.ExpiresAt+1, 0) }
					if err := a.Cleanup(ctx); !errors.Is(err, modelscope.ErrUncertainMutation) {
						t.Fatal(err)
					}
					if _, err := a.S.Get(ctx, f.Hash); err != nil {
						t.Fatal("delete lost metadata", err)
					}
				}
				want := "commit"
				if action == "delete" {
					want = "delete-uncertain"
				}
				if p, err := a.S.Pending(ctx, f.Hash); err != nil || p != want {
					t.Fatal("journal lost", p, err)
				}
				if err := a.S.Release(ctx); err == nil {
					t.Fatal("released uncertain owner")
				}
				f.ExpiresAt = 0
				if _, err := a.Upload(ctx, f, bytes.NewReader(b)); !errors.Is(err, ErrBusy) {
					t.Fatal("renewed uncertain mutation", err)
				}
				if calls != 1 {
					t.Fatal("retried uncertain mutation", calls)
				}
			})
		}
	}
}
func TestDirectPrepareRefusesAuthenticatedLFS(t *testing.T) {
	a, _, _ := setup(t)
	a.C.UploadMode = "direct"
	b := bytes.Repeat([]byte("x"), int(modelscope.InlineLimit)+1)
	f := file(b, 0)
	c := modelscope.New(config.Config{Endpoint: "https://www.modelscope.cn", Repo: "owner/repo", RepoType: "models", UploadMode: "direct", Token: "secret", UploadHosts: []string{".modelscope.cn"}})
	c.API.Transport = apiTransport(func(*http.Request) (*http.Response, error) {
		body := fmt.Sprintf(`{"Code":200,"Data":{"objects":[{"oid":%q,"size":%d,"actions":{"upload":{"href":%q,"offset":0,"upload_header":{"Range":"bytes=0-"},"upload_parameters":{}}}}]}}`, f.Hash, f.Size, "https://lfs.modelscope.cn/api/v1/repos/models/owner/repo/blobs/"+f.Hash+"/"+fmt.Sprint(f.Size))
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	a.U = c
	body, _ := json.Marshal(f)
	rr := httptest.NewRecorder()
	a.Handler(fstest.MapFS{}).ServeHTTP(rr, httptest.NewRequest("POST", "/api/uploads/prepare", bytes.NewReader(body)))
	if rr.Code != 422 || !strings.Contains(rr.Body.String(), "proxy") || strings.Contains(rr.Body.String(), "secret") || strings.Contains(rr.Body.String(), "upload_url") {
		t.Fatal("unsafe direct response", rr.Code, rr.Body.String())
	}
	keys, e := a.S.R.Keys(context.Background(), a.S.Prefix+"session:*").Result()
	if e != nil || len(keys) != 0 {
		t.Fatal("unusable session created", keys, e)
	}
}

func TestUploadModeCSP(t *testing.T) {
	for _, tc := range []struct{ upload, download string }{
		{"proxy", "proxy"},
		{"direct", "proxy"},
		{"proxy", "direct"},
		{"direct", "direct"},
	} {
		t.Run(tc.upload+"/"+tc.download, func(t *testing.T) {
			a, _, _ := setup(t)
			a.C.UploadMode, a.C.DownloadMode = tc.upload, tc.download
			a.C.UploadHosts = []string{".example.com", ".aliyuncs.com"}
			rr := httptest.NewRecorder()
			a.Handler(fstest.MapFS{"index.html": {Data: []byte("UI")}}).ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
			csp := rr.Header().Get("Content-Security-Policy")
			want := "connect-src 'self'"
			// Either direct mode redirects the browser to storage hosts, so both must widen CSP.
			if tc.upload == "direct" || tc.download == "direct" {
				want += " https://example.com https://*.example.com https://aliyuncs.com https://*.aliyuncs.com"
			}
			if !strings.Contains(csp, want+";") || strings.Contains(csp, " https:;") || (tc.upload == "proxy" && tc.download == "proxy") && strings.Contains(csp, "https://*") {
				t.Fatalf("unexpected CSP: %s", csp)
			}
			if !strings.Contains(csp, "frame-ancestors 'none'") || !strings.Contains(csp, "default-src 'self'") {
				t.Fatal("unrelated CSP protections lost", csp)
			}
		})
	}
}

// Direct mode must redirect to the signed URL without proxying bytes; the hash-only
// URL is still a bearer credential, so the response must not be cached.
func TestDownloadDirectRedirects(t *testing.T) {
	a, u, _ := setup(t)
	a.C.DownloadMode = "direct"
	ctx := context.Background()
	b := []byte("direct-mode")
	f := file(b, 0)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	loc := "https://cdn-lfs-cn-1.modelscope.cn/prod/lfs-objects/" + f.Hash + "?auth_key=1"
	u.directLoc = loc
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/download/"+f.Hash, nil)
	a.Handler(fstest.MapFS{}).ServeHTTP(rr, req)
	if rr.Code != 302 || rr.Header().Get("Location") != loc {
		t.Fatalf("no direct redirect: %d %q %s", rr.Code, rr.Header().Get("Location"), rr.Body.String())
	}
	if rr.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("signed URL must not be cached")
	}
}

// Non-LFS/inline files come back as a body, so direct mode must fall back to the
// proxy stream in the same request rather than failing or re-fetching.
func TestDownloadDirectFallsBackToBody(t *testing.T) {
	a, _, _ := setup(t)
	a.C.DownloadMode = "direct"
	ctx := context.Background()
	b := []byte("inline-fallback")
	f := file(b, 0)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	rr := httptest.NewRecorder()
	a.Handler(fstest.MapFS{}).ServeHTTP(rr, httptest.NewRequest("GET", "/api/download/"+f.Hash, nil))
	if rr.Code != 200 || !bytes.Equal(rr.Body.Bytes(), b) {
		t.Fatalf("fallback failed: %d %q", rr.Code, rr.Body.String())
	}
}

// Expiry, short-code validity and the pending journal must still gate direct mode:
// authorization runs before upstream is asked for a URL.
func TestDownloadDirectStillAuthorizes(t *testing.T) {
	a, u, _ := setup(t)
	a.C.DownloadMode = "direct"
	u.directLoc = "https://cdn-lfs-cn-1.modelscope.cn/prod/lfs-objects/x?auth_key=1"
	ctx := context.Background()
	b := []byte("expired-direct")
	f := file(b, a.Now().Unix()+1)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	a.Now = func() time.Time { return time.Unix(f.ExpiresAt+1, 0) }
	rr := httptest.NewRecorder()
	a.Handler(fstest.MapFS{}).ServeHTTP(rr, httptest.NewRequest("GET", "/api/download/"+f.Hash, nil))
	if rr.Code != 404 {
		t.Fatalf("expired direct download allowed: %d %s", rr.Code, rr.Body.String())
	}
}

// The meta document is committed with the blob and is the durable source of the name,
// so a lookup must prefer it over whatever Redis holds.
func TestMetaPathAndNameResolution(t *testing.T) {
	a, u, _ := setup(t)
	u.files = map[string][]byte{}
	ctx := context.Background()
	b := []byte("meta-name")
	f := file(b, 0)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	want := a.C.Namespace + "/meta/" + f.Hash[:2] + "/" + f.Hash[2:] + ".json"
	if u.metaPath != want {
		t.Fatalf("meta committed at %q, want %q", u.metaPath, want)
	}
	if _, _, ok := parseMeta(u.meta); !ok {
		t.Fatalf("committed meta is not a record: %s", u.meta)
	}
	// A stale Redis name must not win over the committed document.
	stale := f
	stale.Name = "stale.txt"
	if e := a.S.Save(ctx, stale); e != nil {
		t.Fatal(e)
	}
	got, e := a.Resolve(ctx, f.Hash)
	if e != nil {
		t.Fatal(e)
	}
	if got.Name != f.Name {
		t.Fatalf("meta name ignored: got %q want %q", got.Name, f.Name)
	}
	// Without a committed document the Redis name is all there is.
	delete(u.files, want)
	got, e = a.Resolve(ctx, f.Hash)
	if e != nil {
		t.Fatal(e)
	}
	if got.Name != "stale.txt" {
		t.Fatalf("Redis name must survive a missing document: %q", got.Name)
	}
}

// A lost Redis record must not lose the file: the committed document reconstructs name
// and size, and the download still succeeds.
func TestResolveRecoversFromCommittedMeta(t *testing.T) {
	a, u, _ := setup(t)
	u.files = map[string][]byte{}
	ctx := context.Background()
	b := []byte("recover-me")
	f := file(b, 0)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	if e := a.S.Remove(ctx, f.Hash); e != nil {
		t.Fatal(e)
	}
	got, e := a.Resolve(ctx, f.Hash)
	if e != nil {
		t.Fatalf("committed meta did not recover the record: %v", e)
	}
	if got.Name != f.Name || got.Size != f.Size || got.ExpiresAt != 0 {
		t.Fatalf("recovered %+v, want name=%q size=%d expiry=0", got, f.Name, f.Size)
	}
	// authorize re-reads Redis; the recovered record must survive that.
	if _, _, e := a.authorize(ctx, f.Hash); e != nil {
		t.Fatalf("recovered file not downloadable: %v", e)
	}
	// A hash with no document stays missing.
	if _, e := a.Resolve(ctx, strings.Repeat("b", 64)); !errors.Is(e, store.ErrMissing) {
		t.Fatalf("unknown hash resolved: %v", e)
	}
}

// An unrelated repository file must not be adopted as a FileCabinet record.
func TestRecoverRejectsForeignContent(t *testing.T) {
	a, u, _ := setup(t)
	ctx := context.Background()
	h := strings.Repeat("c", 64)
	// No type, an unknown type, a bad size, a list that violates its invariants: none of
	// these are FileCabinet records.
	for _, body := range []string{
		"", "not json", `{"message":"nope"}`,
		`{"name":"x","size":7}`,
		`{"type":"other","name":"x","size":7}`,
		`{"type":"file","size":"x"}`, `{"type":"file","size":-1}`,
		`{"type":"file-list","name":"l","size":0,"file-list":["` + h + `"]}`,
		`{"type":"file-list","name":"l","size":7,"file-list":[]}`,
		`{"type":"file-list","name":"l","size":7,"file-list":["nope"]}`,
	} {
		u.files = map[string][]byte{a.MetaPath(h): []byte(body)}
		if _, e := a.Resolve(ctx, h); !errors.Is(e, store.ErrMissing) {
			t.Fatalf("adopted %q: %v", body, e)
		}
	}
	// A file record with an unusable or absent name still recovers; the name falls back
	// to the hash rather than discarding the entry.
	for _, body := range []string{`{"type":"file","name":"","size":7}`, `{"type":"file","name":"a/b.txt","size":7}`, `{"type":"file","size":7}`} {
		u.files = map[string][]byte{a.MetaPath(h): []byte(body)}
		got, e := a.Resolve(ctx, h)
		if e != nil || got.Name != h || got.Size != 7 {
			t.Fatalf("got %+v err=%v for %s", got, e, body)
		}
	}
}

// A failing meta read must degrade to the Redis name, never break the download.
func TestMetaReadFailureKeepsFileAvailable(t *testing.T) {
	a, u, _ := setup(t)
	ctx := context.Background()
	b := []byte("meta-fail")
	f := file(b, 0)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	u.failRead = true
	got, e := a.Resolve(ctx, f.Hash)
	if e != nil {
		t.Fatalf("meta failure broke lookup: %v", e)
	}
	if got.Name != f.Name {
		t.Fatalf("lost the Redis name: %q", got.Name)
	}
}

// Cleanup must remove the document with the blob, and a missing document must not turn
// an empty file into an invalid record.
func TestCleanupRemovesMetaAndEmptyFileRoundTrips(t *testing.T) {
	a, u, _ := setup(t)
	u.files = map[string][]byte{}
	ctx := context.Background()
	sum := sha256.Sum256(nil)
	empty := store.File{Hash: hex.EncodeToString(sum[:]), Name: "empty.bin", Size: 0, ExpiresAt: a.Now().Unix() + 1}
	if _, e := a.Upload(ctx, empty, bytes.NewReader(nil)); e != nil {
		t.Fatal(e)
	}
	if _, _, ok := parseMeta(u.meta); !ok {
		t.Fatal("empty file must still commit a valid record")
	}
	a.Now = func() time.Time { return time.Unix(empty.ExpiresAt+1, 0) }
	if e := a.Cleanup(ctx); e != nil {
		t.Fatal(e)
	}
	if _, ok := u.files[a.MetaPath(empty.Hash)]; ok {
		t.Fatal("cleanup left the meta document behind")
	}
	if p, _ := a.S.Pending(ctx, empty.Hash); p != "" {
		t.Fatalf("journal not cleared: %q", p)
	}
}

// A file list is committed as an index document only: no blob, hash derived from the
// document, and the total is the real sum of its entries.
func TestCreateListCommitsIndexDocument(t *testing.T) {
	a, u, _ := setup(t)
	u.files = map[string][]byte{}
	ctx := context.Background()
	var hashes []string
	var total int64
	for _, body := range [][]byte{[]byte("v1"), []byte("version two"), []byte("v3!")} {
		f := file(body, 0)
		if _, e := a.Upload(ctx, f, bytes.NewReader(body)); e != nil {
			t.Fatal(e)
		}
		hashes = append(hashes, f.Hash)
		total += f.Size
	}
	l, e := a.CreateList(ctx, "各版本高等数学备份", hashes)
	if e != nil {
		t.Fatal(e)
	}
	if l.Size != total {
		t.Fatalf("list size %d, want %d", l.Size, total)
	}
	// The hash is the SHA-256 of the exact committed document bytes, so a client can
	// recompute it from the document it fetches.
	sum := sha256.Sum256(u.meta)
	if l.Hash != hex.EncodeToString(sum[:]) {
		t.Fatalf("list hash %s is not sha256(committed document) %s", l.Hash, u.meta)
	}
	if u.metaPath != a.MetaPath(l.Hash) {
		t.Fatalf("document committed at %q", u.metaPath)
	}
	// The index carries no blob: only the document was written.
	if _, ok := u.files[a.Path(l.Hash)]; ok {
		t.Fatal("list must not write a blob")
	}
	// Re-creating an identical list is idempotent.
	again, e := a.CreateList(ctx, "各版本高等数学备份", hashes)
	if e != nil || again.Hash != l.Hash {
		t.Fatalf("idempotent create failed: %+v %v", again, e)
	}
	// Describe expands entries with their own names and sizes, in document order.
	f, entries, e := a.Describe(ctx, l.Hash)
	if e != nil {
		t.Fatal(e)
	}
	if f.Name != "各版本高等数学备份" || len(entries) != 3 {
		t.Fatalf("describe: %+v %v", f, entries)
	}
	for i, en := range entries {
		if en.Hash != hashes[i] || en.Size == 0 || en.Name == "" {
			t.Fatalf("entry %d: %+v", i, en)
		}
	}
	// A plain file returns no entries.
	if _, entries, e := a.Describe(ctx, hashes[0]); e != nil || entries != nil {
		t.Fatalf("plain file described as list: %v %v", entries, e)
	}
}

func TestCreateListRejectsInvalidInput(t *testing.T) {
	a, u, _ := setup(t)
	u.files = map[string][]byte{}
	ctx := context.Background()
	b := []byte("list member")
	f := file(b, 0)
	if _, e := a.Upload(ctx, f, bytes.NewReader(b)); e != nil {
		t.Fatal(e)
	}
	for name, tc := range map[string]struct {
		name   string
		hashes []string
	}{
		"empty":      {"l", nil},
		"bad name":   {"a/b", []string{f.Hash}},
		"missing":    {"l", []string{strings.Repeat("a", 64)}},
		"duplicate":  {"l", []string{f.Hash, f.Hash}},
		"not a hash": {"l", []string{"nope"}},
	} {
		if _, e := a.CreateList(ctx, tc.name, tc.hashes); !errors.Is(e, ErrInvalid) {
			t.Fatalf("%s accepted: %v", name, e)
		}
	}
	// Too many entries.
	many := make([]string, listLimit+1)
	for i := range many {
		many[i] = strings.Repeat("b", 64)
	}
	if _, e := a.CreateList(ctx, "l", many); !errors.Is(e, ErrInvalid) {
		t.Fatalf("oversized list accepted: %v", e)
	}
	// Nesting is refused so an index stays acyclic.
	inner, e := a.CreateList(ctx, "inner", []string{f.Hash})
	if e != nil {
		t.Fatal(e)
	}
	if _, e := a.CreateList(ctx, "outer", []string{inner.Hash}); !errors.Is(e, ErrInvalid) {
		t.Fatalf("nested list accepted: %v", e)
	}
}

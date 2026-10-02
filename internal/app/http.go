package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"FileCabinet/internal/modelscope"
	"FileCabinet/internal/store"
)

// Lifecycle tracks admitted HTTP requests independently of net/http Shutdown's deadline.
type Lifecycle struct {
	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
}

func (l *Lifecycle) Stop() { l.mu.Lock(); l.closing = true; l.mu.Unlock() }
func (l *Lifecycle) Wait() { l.wg.Wait() }
func (l *Lifecycle) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		if l.closing {
			l.mu.Unlock()
			http.Error(w, "server shutting down", 503)
			return
		}
		l.wg.Add(1)
		l.mu.Unlock()
		defer l.wg.Done()
		next.ServeHTTP(w, r)
	})
}
func (a *Service) Handler(assets fs.FS) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/config", func(w http.ResponseWriter, r *http.Request) {
		reply(w, 200, map[string]any{"upload_mode": a.C.UploadMode, "download_mode": a.C.DownloadMode, "max_upload_bytes": a.C.MaxBytes, "inline_limit": modelscope.InlineLimit, "unsafe_direct_expose_url": a.C.UnsafeDirectExposeURL})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if e := a.S.Check(ctx); e != nil {
			fail(w, e)
			return
		}
		reply(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("PUT /api/files/{hash}", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		size, e := strconv.ParseInt(q.Get("size"), 10, 64)
		if e != nil || size < 0 || size > a.C.MaxBytes {
			fail(w, ErrInvalid)
			return
		}
		expiry, e := parseExpiry(q.Get("expires_at"))
		if e != nil {
			fail(w, e)
			return
		}
		if a.C.UploadMode == "direct" && size > modelscope.InlineLimit {
			fail(w, fmtInvalid("直传模式不支持浏览器大文件上传，请使用原生客户端或切换 proxy"))
			return
		}
		if r.ContentLength >= 0 && r.ContentLength != size {
			fail(w, fmtInvalid("Content-Length 与声明大小不一致"))
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, size+1)
		f, e := a.Upload(r.Context(), store.File{Hash: r.PathValue("hash"), Name: q.Get("name"), Size: size, ExpiresAt: expiry}, r.Body)
		if e != nil {
			fail(w, e)
			return
		}
		reply(w, 201, f)
	})
	mux.HandleFunc("POST /api/uploads/prepare", func(w http.ResponseWriter, r *http.Request) {
		var f store.File
		if e := decode(w, r, &f); e != nil {
			fail(w, e)
			return
		}
		res, e := a.Prepare(r.Context(), f)
		if e != nil {
			fail(w, e)
			return
		}
		reply(w, 201, res)
	})
	mux.HandleFunc("POST /api/uploads/{id}/complete", func(w http.ResponseWriter, r *http.Request) {
		if a.C.UploadMode != "direct" {
			fail(w, ErrInvalid)
			return
		}
		f, e := a.Complete(r.Context(), r.PathValue("id"))
		if e != nil {
			fail(w, e)
			return
		}
		reply(w, 200, f)
	})
	mux.HandleFunc("GET /api/files/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		f, entries, e := a.Describe(r.Context(), key)
		if e != nil {
			fail(w, e)
			return
		}
		out := map[string]any{"file": f}
		if entries == nil {
			out["download_url"] = "/api/download/" + key
		} else {
			out["list"] = entries
		}
		reply(w, 200, out)
	})
	mux.HandleFunc("POST /api/lists", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Name string   `json:"name"`
			List []string `json:"file-list"`
		}
		if e := decode(w, r, &body); e != nil {
			fail(w, e)
			return
		}
		f, e := a.CreateList(r.Context(), body.Name, body.List)
		if e != nil {
			fail(w, e)
			return
		}
		reply(w, 201, f)
	})
	mux.HandleFunc("POST /api/codes", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Hash      string `json:"hash"`
			ExpiresAt int64  `json:"expires_at"`
		}
		if e := decode(w, r, &body); e != nil {
			fail(w, e)
			return
		}
		c, e := a.NewCode(r.Context(), body.Hash, body.ExpiresAt)
		if e != nil {
			fail(w, e)
			return
		}
		reply(w, 201, c)
	})
	mux.HandleFunc("GET /api/download/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if a.C.DownloadMode == "direct" {
			f, loc, resp, e := a.DirectDownload(r.Context(), key)
			if e != nil {
				fail(w, e)
				return
			}
			if resp == nil {
				// Signed upstream URL: the browser fetches bytes from storage directly.
				w.Header().Set("Cache-Control", "no-store")
				http.Redirect(w, r, loc, http.StatusFound)
				return
			}
			defer resp.Body.Close()
			streamFile(w, f, resp)
			return
		}
		f, resp, e := a.Open(r.Context(), key)
		if e != nil {
			fail(w, e)
			return
		}
		defer resp.Body.Close()
		streamFile(w, f, resp)
	})
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		reply(w, 404, map[string]string{"error": "接口不存在"})
	})
	files := http.FileServer(http.FS(assets))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			http.Error(w, "method not allowed", 405)
			return
		}
		if r.URL.Path != "/" {
			name := strings.TrimPrefix(r.URL.Path, "/")
			if _, e := fs.Stat(assets, name); e != nil {
				http.NotFound(w, r)
				return
			}
		}
		files.ServeHTTP(w, r)
	})
	// Config validates host suffix syntax. CSP permits only HTTPS hosts accepted by
	// the storage allowlist, and only when a direct (browser-facing) mode is enabled.
	connectSrc := "'self'"
	if a.C.UploadMode == "direct" || a.C.DownloadMode == "direct" {
		for _, suffix := range a.C.UploadHosts {
			host := strings.TrimPrefix(suffix, ".")
			connectSrc += " https://" + host + " https://*." + host
		}
	}
	slots := make(chan struct{}, 16)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; worker-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src "+connectSrc+"; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		// Disallow browser cross-site writes; deploy behind authentication for untrusted networks.
		if r.Method != "GET" && r.Method != "HEAD" {
			if origin := r.Header.Get("Origin"); origin != "" {
				scheme := "http"
				if r.TLS != nil {
					scheme = "https"
				}
				if origin != scheme+"://"+r.Host { // reverse proxies should preserve Host and set the original protocol
					if !(r.Header.Get("X-Forwarded-Proto") == "https" && origin == "https://"+r.Host) {
						reply(w, 403, map[string]string{"error": "不允许跨站写入"})
						return
					}
				}
			}
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			reply(w, 503, map[string]string{"error": "并发任务已满，请稍后重试"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), a.C.Timeout)
		defer cancel()
		rc := http.NewResponseController(w)
		_ = rc.SetReadDeadline(time.Now().Add(a.C.Timeout))
		_ = rc.SetWriteDeadline(time.Now().Add(a.C.Timeout))
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func parseExpiry(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}
	n, e := strconv.ParseInt(v, 10, 64)
	if e != nil {
		return 0, ErrInvalid
	}
	return n, nil
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return ErrInvalid
	}
	if e := d.Decode(&struct{}{}); e != io.EOF {
		return ErrInvalid
	}
	return nil
}
func fmtInvalid(s string) error { return errors.Join(ErrInvalid, errors.New(s)) }
func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// streamFile relays upstream bytes with our own headers. The upstream body stays
// owned by the caller; callers must close it.
func streamFile(w http.ResponseWriter, f store.File, resp *http.Response) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": f.Name}))
	w.Header().Set("Content-Length", strconv.FormatInt(f.Size, 10))
	w.WriteHeader(200)
	if n, e := io.Copy(w, io.LimitReader(resp.Body, f.Size)); e != nil || n != f.Size {
		slog.Warn("download stream interrupted")
	}
}
func fail(w http.ResponseWriter, e error) {
	status := 503
	msg := "服务暂不可用，请稍后重试或联系管理员"
	switch {
	case errors.Is(e, ErrInvalid):
		status = 400
		msg = e.Error()
	case errors.Is(e, store.ErrMissing):
		status = 404
		msg = store.ErrMissing.Error()
	case errors.Is(e, ErrBusy):
		status = 409
		msg = ErrBusy.Error()
	case errors.Is(e, modelscope.ErrDirectUnsupported):
		status = 422
		msg = modelscope.ErrDirectUnsupported.Error()
	case errors.Is(e, modelscope.ErrUpstream):
		status = 502
		msg = modelscope.ErrUpstream.Error()
	case errors.Is(e, context.DeadlineExceeded):
		status = 504
		msg = "操作超时，请查询结果后重试"
	}
	reply(w, status, map[string]string{"error": msg})
}

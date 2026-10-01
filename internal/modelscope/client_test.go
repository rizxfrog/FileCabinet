package modelscope

import (
	"FileCabinet/internal/config"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(body string) *http.Response {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}
func TestWireProtocol(t *testing.T) {
	for _, kind := range []string{"models", "datasets"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			hash := strings.Repeat("a", 64)
			var requests int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Header.Get("Authorization") != "Bearer secret" {
					t.Error("missing bearer")
				}
				cookie, e := r.Cookie("m_session_id")
				if e != nil || cookie.Value != "secret" {
					t.Error("missing cookie")
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/objects/batch"):
					if r.Method != "POST" || r.URL.Path != "/api/v1/repos/"+kind+"/owner/repo/info/lfs/objects/batch" {
						t.Error(r.URL)
					}
					var b struct {
						Operation string           `json:"operation"`
						Objects   []map[string]any `json:"objects"`
					}
					_ = json.NewDecoder(r.Body).Decode(&b)
					if b.Operation != "upload" || b.Objects[0]["oid"] != hash {
						t.Error(b)
					}
					io.WriteString(w, `{"Code":200,"Data":{"objects":[{"oid":"`+hash+`","size":6000000,"actions":{"upload":{"href":"https://bucket.oss.aliyuncs.com/upload?signature=abc"}}}]}}`)
				case strings.Contains(r.URL.Path, "/commit/"):
					if r.Method != "POST" || r.URL.Path != "/api/v1/repos/"+kind+"/owner/repo/commit/master" {
						t.Error(r.URL)
					}
					var b struct {
						Actions []map[string]any `json:"actions"`
					}
					_ = json.NewDecoder(r.Body).Decode(&b)
					a := b.Actions[0]
					if a["action"] != "create" || a["path"] != "00/aa/path" {
						t.Error(a)
					}
					if a["type"] == "normal" {
						if a["content"] != "aGk=" || a["sha256"] != "" || a["encoding"] != "base64" {
							t.Error(a)
						}
					} else {
						if a["sha256"] != hash || a["content"] != "" || a["encoding"] != "" {
							t.Error(a)
						}
					}
					io.WriteString(w, `{"Code":200,"Success":true,"Data":{}}`)
				case r.Method == "DELETE":
					suffix := "file"
					if kind == "datasets" {
						suffix = "repo"
						if r.URL.Query().Has("Revision") {
							t.Error("dataset delete must not send revision")
						}
					} else if r.URL.Query().Get("Revision") != "master" {
						t.Error("model revision missing")
					}
					if r.URL.Path != "/api/v1/"+kind+"/owner/repo/"+suffix || r.URL.Query().Get("FilePath") != "00/aa/path" {
						t.Error(r.URL)
					}
					io.WriteString(w, `{"Code":200,"Data":{}}`)
				case r.Method == "GET":
					if r.URL.Path != "/api/v1/"+kind+"/owner/repo/repo" || r.URL.Query().Get("Revision") != "master" || r.URL.Query().Get("FilePath") != "00/aa/path" {
						t.Error(r.URL)
					}
					io.WriteString(w, "hi")
				default:
					t.Error("unexpected request", r.URL)
				}
			}))
			defer server.Close()
			c := New(config.Config{Endpoint: server.URL, Token: "secret", Repo: "owner/repo", RepoType: kind, Revision: "master", UploadHosts: []string{".aliyuncs.com"}, Timeout: time.Second})
			c.API = server.Client()
			target, e := c.Validate(ctx, hash, 6000000)
			if e != nil || !strings.Contains(target, "signature=") {
				t.Fatal(target, e)
			}
			if e = c.Commit(ctx, "00/aa/path", hash, 2, []byte("hi")); e != nil {
				t.Fatal(e)
			}
			if e = c.Commit(ctx, "00/aa/path", hash, 6000000, nil); e != nil {
				t.Fatal(e)
			}
			if e = c.Delete(ctx, "00/aa/path"); e != nil {
				t.Fatal(e)
			}
			resp, e := c.Download(ctx, "00/aa/path")
			if e != nil {
				t.Fatal(e)
			}
			resp.Body.Close()
			if requests != 5 {
				t.Fatal(requests)
			}
		})
	}
}
func TestValidateTreatsOmittedObjectAsStored(t *testing.T) {
	hash := strings.Repeat("a", 64)
	// Real upstream/shape: ModelScope replies 200 with the object missing from
	// "objects" when the blob is already stored. The SDK documents the same
	// contract (href None = blob exists), so this must not be an error.
	for _, body := range []string{
		`{"Code":200,"Success":true,"Message":"success","Data":{"objects":[],"reused-objects":[]}}`,
		`{"Code":200,"Success":true,"Message":"success","Data":{"objects":[],"reused-objects":[{"oid":"` + hash + `"}]}}`,
	} {
		c := New(config.Config{Endpoint: "https://example.com", Repo: "o/r", RepoType: "datasets", Revision: "master", UploadHosts: []string{".aliyuncs.com"}})
		c.API = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) { return response(body), nil })}
		target, e := c.Validate(context.Background(), hash, 351939402)
		if e != nil {
			t.Fatalf("omitted object must mean already stored, got %v for %s", e, body)
		}
		if target != "" {
			t.Fatalf("stored blob must not return an upload target, got %q", target)
		}
	}
}

func TestValidateFailsClosed(t *testing.T) {
	hash := strings.Repeat("f", 64)
	for _, body := range []string{`{}`, `{"Code":500,"Data":{}}`, `{"Success":false}`, `{"objects":[{"oid":"` + hash + `","error":{"code":404}}]}`, `{"objects":[{"oid":"` + hash + `","actions":{"upload":{"href":"http://127.0.0.1/x"}}}]}`, `{"objects":[{"oid":"` + hash + `","actions":{"upload":{"href":"https://bucket.aliyuncs.com/x","header":{"Authorization":"secret"}}}}]}`} {
		c := New(config.Config{Endpoint: "https://example.com", Repo: "o/r", RepoType: "models", UploadHosts: []string{".aliyuncs.com"}})
		c.API = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) { return response(body), nil })}
		if _, e := c.Validate(context.Background(), hash, 1); e == nil {
			t.Fatal("accepted malformed batch", body)
		}
	}
}
func TestStorageNoTokenAndDownloadRedirect(t *testing.T) {
	c := New(config.Config{Endpoint: "https://www.modelscope.cn", Token: "secret", Repo: "o/r", RepoType: "models", Revision: "master", UploadHosts: []string{".aliyuncs.com"}})
	c.Storage = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Fatal("token leaked")
		}
		if r.URL.Host != "bucket.aliyuncs.com" {
			t.Fatal(r.URL)
		}
		if r.Method == "PUT" {
			b, _ := io.ReadAll(r.Body)
			if string(b) != "abc" || r.ContentLength != 3 {
				t.Fatal("put payload")
			}
		}
		return response("abc"), nil
	})}
	if e := c.Put(context.Background(), "https://bucket.aliyuncs.com/blob", strings.Repeat("a", 64), strings.NewReader("abc"), 3); e != nil {
		t.Fatal(e)
	}
	c.API = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatal("no auth")
		}
		return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"https://bucket.aliyuncs.com/file"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}
	resp, e := c.Download(context.Background(), "00/aa/path")
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	for _, raw := range []string{"http://bucket.aliyuncs.com/", "https://127.0.0.1/", "https://bucket.aliyuncs.com.evil.com/", "https://user:pass@bucket.aliyuncs.com/", "https://bucket.aliyuncs.com:8443/"} {
		if c.SafeStorageURL(raw) {
			t.Fatal("unsafe URL allowed", raw)
		}
	}
}

func TestDeleteOutcomeClassification(t *testing.T) {
	for _, status := range []int{403, 429, 500, 408, 200, 404} {
		c := New(config.Config{Endpoint: "https://www.modelscope.cn", Repo: "o/r", RepoType: "models", Revision: "master"})
		c.API = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
			r := response(`{"Code":200,"Success":true}`)
			r.StatusCode = status
			return r, nil
		})}
		err := c.Delete(context.Background(), "00/aa/path")
		if status == 500 || status == 408 {
			if !errors.Is(err, ErrUncertainMutation) {
				t.Fatal("unknown delete not quarantined", status, err)
			}
		} else if status == 403 || status == 429 {
			if !errors.Is(err, ErrUpstream) || errors.Is(err, ErrUncertainMutation) {
				t.Fatal("definite rejection classification", status, err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}

// Captured ModelScope batch shape, not an invented cloud presigned response.
func lfsFixture(hash, target string, size int64) string {
	return fmt.Sprintf(`{"Code":200,"Success":true,"Message":"success","Data":{"objects":[{"oid":%q,"size":%d,"actions":{"upload":{"href":%q,"offset":0,"upload_header":{"Range":"bytes=0-"},"upload_parameters":{}}}}],"reused-objects":[]}}`, hash, size, target)
}

// Captured ModelScope download redirect shape: the API answers 302 to a CDN URL
// carrying an opaque auth_key plus repository layout probes we must strip.
func TestDownloadTargetRedirectAndProbes(t *testing.T) {
	hash := strings.Repeat("a", 64)
	// Real upstream shape: bucket prefix, hash split across segments, layout probes,
	// and the opaque signature.
	cdn := "https://cdn-lfs-cn-1.modelscope.cn/prod/lfs-objects/aa/aa/" + hash +
		"?filename=" + hash + "&namespace=owner&repository=repo&revision=deadbeef&tag=dataset&auth_key=123-abc"
	c := New(config.Config{Endpoint: "https://www.modelscope.cn", Token: "secret", Repo: "owner/repo", RepoType: "datasets", Revision: "master", UploadHosts: []string{".modelscope.cn"}})
	var gotURL string
	c.API = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: transport(func(r *http.Request) (*http.Response, error) {
		gotURL = r.URL.String()
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatal("api call unauthenticated")
		}
		return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{cdn}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}
	loc, resp, e := c.DownloadTarget(context.Background(), "00/aa/path", hash, "novelread.apk")
	if e != nil || resp != nil {
		t.Fatal(loc, resp, e)
	}
	if !strings.Contains(gotURL, "/api/v1/datasets/owner/repo/repo") || !strings.Contains(gotURL, "FilePath=00%2Faa%2Fpath") {
		t.Fatal(gotURL)
	}
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatal(err)
	}
	if q := u.Query(); q.Get("auth_key") != "123-abc" || q.Get("namespace") != "" || q.Get("repository") != "" || q.Get("revision") != "" || q.Get("tag") != "" {
		t.Fatalf("layout probes not stripped, signature not preserved: %s", loc)
	} else if len(q) != 2 || q.Get("filename") != "novelread.apk" {
		// filename is not covered by the signature, so it carries the real name while
		// every repository layout probe is removed.
		t.Fatalf("unexpected query params: %s", loc)
	}
}

// A misconfigured or hostile upstream must not be able to redirect to a different
// object, another host outside the allowlist, or a plain-HTTP target.
func TestDownloadTargetRejectsUnsafeRedirects(t *testing.T) {
	hash := strings.Repeat("a", 64)
	for _, raw := range []string{
		"https://cdn-lfs-cn-1.modelscope.cn/prod/lfs-objects/aa/aa/" + strings.Repeat("b", 60) + "?auth_key=1",
		"https://evil.example/prod/" + hash + "?auth_key=1",
		"https://cdn.evil-modelscope.cn/" + hash + "?auth_key=1",
		"http://cdn-lfs-cn-1.modelscope.cn/" + hash + "?auth_key=1",
		"https://user:pass@cdn-lfs-cn-1.modelscope.cn/" + hash,
		"https://cdn-lfs-cn-1.modelscope.cn:8443/" + hash,
	} {
		c := New(config.Config{Endpoint: "https://www.modelscope.cn", Token: "secret", Repo: "owner/repo", RepoType: "datasets", Revision: "master", UploadHosts: []string{".modelscope.cn"}})
		c.API = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: transport(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{raw}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		})}
		if loc, resp, e := c.DownloadTarget(context.Background(), "00/aa/path", hash, "novel.apk"); e == nil || loc != "" || resp != nil {
			t.Fatalf("unsafe redirect accepted: %s", raw)
		}
	}
}

// Upstream echoes the filename query value verbatim into the CDN Content-Disposition
// header, so the encoding we choose is what the browser ends up showing. Raw UTF-8 or
// spaces would make the URL invalid, percent-encoding is decoded by browsers, and "+"
// must be avoided because the CDN echoes it literally as "my+file.apk".
func TestDownloadTargetFilenameEncoding(t *testing.T) {
	hash := strings.Repeat("a", 64)
	cdn := "https://cdn-lfs-cn-1.modelscope.cn/prod/lfs-objects/aa/aa/" + hash + "?auth_key=123-abc&filename=" + hash
	for _, tc := range []struct{ name, want string }{
		{"说明.txt", "说明.txt"},
		{"my file.apk", "my file.apk"},
		{"a+b.apk", "a+b.apk"},
		{"novelread.apk", "novelread.apk"},
		// Rejected names fall back to the hash the caller already knows.
		{"../../etc/passwd", hash},
		{"bad\r\nX: y", hash},
		{"a\"b.apk", hash},
		{"", hash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(config.Config{Endpoint: "https://www.modelscope.cn", Token: "secret", Repo: "owner/repo", RepoType: "datasets", Revision: "master", UploadHosts: []string{".modelscope.cn"}})
			c.API = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: transport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{cdn}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
			})}
			loc, resp, e := c.DownloadTarget(context.Background(), "00/aa/path", hash, tc.name)
			if e != nil || resp != nil {
				t.Fatal(loc, resp, e)
			}
			u, err := url.Parse(loc)
			if err != nil {
				t.Fatal(err)
			}
			// The decoded value is the filename the browser will show.
			if got := u.Query().Get("filename"); got != tc.want {
				t.Fatalf("filename=%q, want %q (%s)", got, tc.want, loc)
			}
			// Pin the exact wire bytes: url.QueryEscape turns a space into "+", which the
			// CDN echoes literally ("my+file.apk"), so the client must emit %20 instead.
			wire := strings.ReplaceAll(url.QueryEscape(tc.want), "+", "%20")
			if !strings.Contains(loc, "filename="+wire+"&") && !strings.HasSuffix(loc, "filename="+wire) {
				t.Fatalf("filename not encoded as %q: %s", wire, loc)
			}
		})
	}
}

// Small/inline and other non-LFS files are answered with the body itself, so the
// caller must be handed a stream instead of a URL and must not retry upstream.
func TestDownloadTargetInlineBody(t *testing.T) {
	hash := strings.Repeat("a", 64)
	calls := 0
	c := New(config.Config{Endpoint: "https://www.modelscope.cn", Token: "secret", Repo: "owner/repo", RepoType: "datasets", Revision: "master", UploadHosts: []string{".modelscope.cn"}})
	c.API = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: transport(func(r *http.Request) (*http.Response, error) {
		calls++
		return response("hello"), nil
	})}
	loc, resp, e := c.DownloadTarget(context.Background(), "00/aa/path", hash, "novel.apk")
	if e != nil || loc != "" || resp == nil || calls != 1 {
		t.Fatal(loc, resp, e, calls)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "hello" {
		t.Fatal(string(b))
	}
}
func TestLFSUploadAuthenticationAndDirectRefusal(t *testing.T) {
	for _, kind := range []string{"models", "datasets"} {
		t.Run(kind, func(t *testing.T) {
			hash := strings.Repeat("b", 64)
			target := "https://lfs.modelscope.cn/api/v1/repos/" + kind + "/owner/repo/blobs/" + hash + "/6000000"
			c := New(config.Config{Endpoint: "https://www.modelscope.cn", Token: "secret", Repo: "owner/repo", RepoType: kind, UploadMode: "proxy", UploadHosts: []string{".modelscope.cn", ".aliyuncs.com"}})
			c.API.Transport = transport(func(*http.Request) (*http.Response, error) { return response(lfsFixture(hash, target, 6000000)), nil })
			putCalls := 0
			c.Storage.Transport = transport(func(r *http.Request) (*http.Response, error) {
				putCalls++
				cookie, err := r.Cookie("m_session_id")
				if r.URL.String() != target || r.Method != "PUT" || r.ContentLength != 6000000 || r.Header.Get("Authorization") != "Bearer secret" || err != nil || cookie.Value != "secret" {
					t.Fatal("wrong LFS request", r.URL, r.Header)
				}
				// Neither credentials nor the body may follow a redirect, even to another allowlisted host.
				if putCalls == 2 {
					res := response("")
					res.StatusCode = 307
					res.Header.Set("Location", "https://bucket.aliyuncs.com/leak")
					return res, nil
				}
				return response(`{"Code":200,"Success":true,"Data":{}}`), nil
			})
			got, err := c.Validate(context.Background(), hash, 6000000)
			if err != nil || got != target {
				t.Fatal(got, err)
			}
			if err = c.Put(context.Background(), got, hash, strings.NewReader("payload"), 6000000); err != nil {
				t.Fatal(err)
			}
			if err = c.Put(context.Background(), got, hash, strings.NewReader("payload"), 6000000); err == nil || putCalls != 2 {
				t.Fatal("LFS redirect followed or accepted", putCalls, err)
			}
			c.C.UploadMode = "direct"
			if got, err = c.Validate(context.Background(), hash, 6000000); !errors.Is(err, ErrDirectUnsupported) || got != "" {
				t.Fatal("returned authentication-required target", got, err)
			}
			c.API.Transport = transport(func(*http.Request) (*http.Response, error) {
				return response(`{"Code":200,"Data":{"objects":[{"oid":"` + hash + `","size":6000000}]}}`), nil
			})
			if got, err = c.Validate(context.Background(), hash, 6000000); err != nil || got != "" {
				t.Fatal("existing blob reuse rejected", got, err)
			}
			c.API.Transport = transport(func(*http.Request) (*http.Response, error) {
				return response(lfsFixture(hash, "https://bucket.aliyuncs.com/blob?signature=abc", 6000000)), nil
			})
			if got, err = c.Validate(context.Background(), hash, 6000000); err != nil || got == "" {
				t.Fatal("signed native target rejected", got, err)
			}
		})
	}
}
func TestLFSRejectsMismatchedTargetsBeforeSending(t *testing.T) {
	hash := strings.Repeat("a", 64)
	target := "https://lfs.modelscope.cn/api/v1/repos/models/owner/repo/blobs/" + hash + "/6"
	c := New(config.Config{Endpoint: "https://www.modelscope.cn", Token: "secret", Repo: "owner/repo", RepoType: "models", UploadHosts: []string{".modelscope.cn"}})
	c.Storage.Transport = transport(func(*http.Request) (*http.Response, error) { t.Fatal("untrusted upload sent"); return nil, nil })
	for _, raw := range []string{
		strings.Replace(target, "lfs.modelscope.cn", "evil.modelscope.cn", 1),
		strings.Replace(target, "lfs.modelscope.cn", "lfs.modelscope.cn.evil.com", 1),
		strings.Replace(target, "/models/", "/datasets/", 1),
		strings.Replace(target, "owner/repo", "other/repo", 1),
		strings.Replace(target, hash, strings.Repeat("f", 64), 1),
		target + "0", target + "?signature=abc", target + "#fragment",
		strings.Replace(target, "https://", "http://", 1),
		strings.Replace(target, "/owner/", "/%6fwner/", 1),
		strings.Replace(target, "/blobs/", "/x/../blobs/", 1),
	} {
		c.API.Transport = transport(func(*http.Request) (*http.Response, error) { return response(lfsFixture(hash, raw, 6)), nil })
		if got, err := c.Validate(context.Background(), hash, 6); err == nil || got != "" {
			t.Fatal("batch accepted", raw)
		}
		if err := c.Put(context.Background(), raw, hash, strings.NewReader("abcdef"), 6); err == nil {
			t.Fatal("put accepted", raw)
		}
	}
}
func TestMutationsRequireExplicitSuccess(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `[]`, `{"Data":{}}`, `{"Success":true}`, `{"Code":200,"Success":null}`, `{"Code":200,"Success":"true"}`, `{"Code":200,"Success":1}`, `{"Code":200,"Success":false}`, `{"Code":500,"Success":true}`, `{"Code":null}`, `{"Code":true}`, `{"Code":200}garbage`} {
		t.Run(body, func(t *testing.T) {
			c := New(config.Config{Endpoint: "https://www.modelscope.cn", Repo: "o/r", RepoType: "models", Revision: "master"})
			c.API.Transport = transport(func(*http.Request) (*http.Response, error) { return response(body), nil })
			for _, err := range []error{c.Commit(context.Background(), "path", strings.Repeat("a", 64), 0, nil), c.Delete(context.Background(), "path")} {
				if !errors.Is(err, ErrUncertainMutation) {
					t.Fatal("unconfirmed mutation succeeded", body, err)
				}
			}
		})
	}
	for _, body := range []string{`{"Code":200}`, `{"Code":"200","Success":true,"Data":{}}`} {
		c := New(config.Config{Endpoint: "https://www.modelscope.cn"})
		c.API.Transport = transport(func(*http.Request) (*http.Response, error) { return response(body), nil })
		if err := c.Commit(context.Background(), "path", strings.Repeat("a", 64), 0, nil); err != nil {
			t.Fatal(body, err)
		}
		if err := c.Delete(context.Background(), "path"); err != nil {
			t.Fatal(body, err)
		}
	}
}
func TestEmptyPUTResponseIsOnlyValidForStorage(t *testing.T) {
	hash := strings.Repeat("a", 64)
	c := New(config.Config{Token: "secret", Repo: "o/r", RepoType: "models", UploadHosts: []string{".modelscope.cn", ".aliyuncs.com"}})
	c.Storage.Transport = transport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "bucket.aliyuncs.com" && (r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "") {
			t.Fatal("storage token leaked")
		}
		return response(""), nil
	})
	if err := c.Put(context.Background(), "https://bucket.aliyuncs.com/blob?signature=abc", hash, strings.NewReader("abc"), 3); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(context.Background(), "https://lfs.modelscope.cn/api/v1/repos/models/o/r/blobs/"+hash+"/3", hash, strings.NewReader("abc"), 3); err == nil {
		t.Fatal("LFS unconfirmed response accepted")
	}
}

func TestOversizedMutationEnvelopeCannotHideTrailingData(t *testing.T) {
	c := New(config.Config{Endpoint: "https://www.modelscope.cn"})
	body := `{"Code":200}` + strings.Repeat(" ", 2<<20) + `invalid trailing data`
	c.API.Transport = transport(func(*http.Request) (*http.Response, error) { return response(body), nil })
	for _, err := range []error{c.Commit(context.Background(), "path", strings.Repeat("a", 64), 0, nil), c.Delete(context.Background(), "path")} {
		if !errors.Is(err, ErrUncertainMutation) {
			t.Fatal("truncated response confirmed mutation", err)
		}
	}
}

package modelscope

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"FileCabinet/internal/config"
)

const InlineLimit int64 = 5 * 1024 * 1024

var ErrDirectUnsupported = errors.New("上游上传地址需要服务端认证，不支持免 token 直传；请使用 UPLOAD_MODE=proxy")

var ErrUpstream = errors.New("ModelScope 请求失败，请稍后重试或联系管理员")

// A transport failure or ambiguous response cannot prove a remote mutation stopped.
var ErrUncertainMutation = errors.Join(ErrUpstream, errors.New("remote mutation outcome is uncertain"))

type Client struct {
	C            config.Config
	API, Storage *http.Client
}

func New(c config.Config) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = 60 * time.Second
	tr.MaxIdleConnsPerHost = 16
	api := &http.Client{Transport: tr, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	// Validate every storage destination at connection time, not just before DNS resolution.
	st := tr.Clone()
	st.Proxy = nil
	st.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(addr)
		if e != nil {
			return nil, e
		}
		ips, e := net.DefaultResolver.LookupIPAddr(ctx, host)
		if e != nil {
			return nil, e
		}
		for _, ip := range ips {
			if !publicIP(ip.IP) {
				return nil, ErrUpstream
			}
		}
		if len(ips) == 0 {
			return nil, ErrUpstream
		}
		return (&net.Dialer{Timeout: 30 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}
	return &Client{C: c, API: api, Storage: &http.Client{Transport: st, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func publicIP(ip net.IP) bool {
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !net.ParseIP("100.64.0.0").Equal(ip) && !inCGN(ip)
}
func inCGN(ip net.IP) bool { _, n, _ := net.ParseCIDR("100.64.0.0/10"); return n.Contains(ip) }
func (c *Client) SafeStorageURL(raw string) bool {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.User != nil || u.Hostname() == "" || (u.Port() != "" && u.Port() != "443") || u.Fragment != "" {
		return false
	}
	h := strings.ToLower(u.Hostname())
	if net.ParseIP(h) != nil {
		return false
	}
	for _, s := range c.C.UploadHosts {
		if strings.HasSuffix(h, s) || h == strings.TrimPrefix(s, ".") {
			return true
		}
	}
	return false
}

// Only this exact ModelScope LFS route may receive the backend credential.
// Storage allowlisting alone is never authority to disclose a token.
func (c *Client) uploadTarget(raw, hash string, size int64) (authenticated, valid bool) {
	if !c.SafeStorageURL(raw) || size < 0 || !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(hash) {
		return false, false
	}
	expected := "https://lfs.modelscope.cn/api/v1/repos/" + c.C.RepoType + "/" + c.C.Repo + "/blobs/" + hash + "/" + strconv.FormatInt(size, 10)
	if raw == expected {
		return true, true
	}
	u, _ := url.Parse(raw)
	host := strings.ToLower(u.Hostname())
	if host == "modelscope.cn" || strings.HasSuffix(host, ".modelscope.cn") {
		return false, false
	}
	return false, true
}

// A mutation is confirmed only by an explicit, non-conflicting API envelope.
func successEnvelope(b []byte) (map[string]json.RawMessage, bool) {
	var env map[string]json.RawMessage
	if json.Unmarshal(b, &env) != nil || env == nil {
		return nil, false
	}
	code := string(env["Code"])
	if code != "200" && code != `"200"` {
		return nil, false
	}
	if success, ok := env["Success"]; ok && string(success) != "true" {
		return nil, false
	}
	return env, true
}
func (c *Client) path(s string) string { return strings.TrimRight(c.C.Endpoint, "/") + "/api/v1/" + s }
func (c *Client) auth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.C.Token)
	req.AddCookie(&http.Cookie{Name: "m_session_id", Value: c.C.Token})
}
func (c *Client) json(ctx context.Context, method, path string, payload any, out any) error {
	var body io.Reader
	if payload != nil {
		b, e := json.Marshal(payload)
		if e != nil {
			return e
		}
		body = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(ctx, method, c.path(path), body)
	if e != nil {
		return ErrUpstream
	}
	c.auth(req)
	req.Header.Set("Content-Type", "application/json")
	resp, e := c.API.Do(req)
	if e != nil {
		return ErrUpstream
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ErrUpstream
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, (2<<20)+1))
	if e != nil || len(b) > 2<<20 {
		return ErrUpstream
	}
	env, ok := successEnvelope(b)
	if !ok {
		return ErrUpstream
	}
	if data, ok := env["Data"]; ok {
		b = data
	}
	if out != nil && json.Unmarshal(b, out) != nil {
		return ErrUpstream
	}
	return nil
}
func (c *Client) Validate(ctx context.Context, hash string, size int64) (string, error) {
	var data struct {
		Objects []struct {
			OID     string          `json:"oid"`
			Size    int64           `json:"size"`
			Error   json.RawMessage `json:"error"`
			Actions map[string]struct {
				Href             string            `json:"href"`
				Header           map[string]string `json:"header"`
				UploadHeader     map[string]string `json:"upload_header"`
				UploadParameters map[string]string `json:"upload_parameters"`
				Offset           int64             `json:"offset"`
			} `json:"actions"`
		} `json:"objects"`
	}
	e := c.json(ctx, "POST", "repos/"+c.C.RepoType+"/"+c.C.Repo+"/info/lfs/objects/batch", map[string]any{"operation": "upload", "objects": []any{map[string]any{"oid": hash, "size": size}}}, &data)
	if e != nil {
		return "", e
	}
	for _, o := range data.Objects {
		if o.OID == hash {
			if len(o.Error) > 0 && string(o.Error) != "null" {
				return "", ErrUpstream
			}
			if o.Size != 0 && o.Size != size {
				return "", ErrUpstream
			}
			if a, ok := o.Actions["upload"]; ok {
				auth, valid := c.uploadTarget(a.Href, hash, size)
				if !valid || len(a.Header) > 0 || len(a.UploadParameters) > 0 || a.Offset != 0 || (len(a.UploadHeader) > 0 && (len(a.UploadHeader) != 1 || a.UploadHeader["Range"] != "bytes=0-")) {
					return "", ErrUpstream
				}
				if c.C.UploadMode == "direct" && auth && !c.C.UnsafeDirectExposeURL {
					return "", ErrDirectUnsupported
				}
				return a.Href, nil
			}
			return "", nil
		}
	}
	// ModelScope omits the requested object from the batch reply when the blob is
	// already stored (the official SDK documents the same: an oid absent from
	// objects means "exists", with no separate flag). An empty list is a successful
	// "nothing to upload", not a malformed reply. Returning "" makes callers skip the
	// PUT and commit the pointer; a rewritten or mismatched upload action is still
	// rejected strictly above.
	return "", nil
}
func (c *Client) Put(ctx context.Context, raw, hash string, r io.Reader, size int64) error {
	auth, valid := c.uploadTarget(raw, hash, size)
	if !valid {
		return ErrUpstream
	}
	req, e := http.NewRequestWithContext(ctx, "PUT", raw, r)
	if e != nil {
		return ErrUpstream
	}
	if auth {
		c.auth(req)
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, e := c.Storage.Do(req)
	if e != nil {
		return ErrUpstream
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ErrUpstream
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if e != nil || len(b) > 64<<10 {
		return ErrUpstream
	}
	if auth {
		if _, ok := successEnvelope(b); !ok {
			return ErrUpstream
		}
		return nil
	}
	if len(bytes.TrimSpace(b)) > 0 {
		var env map[string]json.RawMessage
		if json.Unmarshal(b, &env) == nil {
			if code, ok := env["Code"]; ok && string(code) != "200" && string(code) != `"200"` {
				return ErrUpstream
			}
		}
	}
	return nil
}
func (c *Client) Commit(ctx context.Context, path, hash string, size int64, content []byte) error {
	op := map[string]any{"action": "create", "path": path, "type": "lfs", "size": size, "sha256": hash, "content": "", "encoding": ""}
	if size <= InlineLimit {
		op["type"] = "normal"
		op["sha256"] = ""
		op["content"] = base64.StdEncoding.EncodeToString(content)
		op["encoding"] = "base64"
	}
	err := c.json(ctx, "POST", "repos/"+c.C.RepoType+"/"+c.C.Repo+"/commit/"+url.PathEscape(c.C.Revision), map[string]any{"commit_message": "FileCabinet store " + hash, "actions": []any{op}}, nil)
	if err != nil {
		return ErrUncertainMutation
	}
	return nil
}
func (c *Client) Delete(ctx context.Context, path string) error {
	suffix := "file"
	q := url.Values{"FilePath": {path}, "Revision": {c.C.Revision}}
	if c.C.RepoType == "datasets" {
		suffix = "repo"
		q.Del("Revision")
	}
	req, e := http.NewRequestWithContext(ctx, "DELETE", c.path(c.C.RepoType+"/"+c.C.Repo+"/"+suffix)+"?"+q.Encode(), nil)
	if e != nil {
		return ErrUpstream
	}
	c.auth(req)
	resp, e := c.API.Do(req)
	if e != nil {
		return ErrUncertainMutation
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil
	}
	if resp.StatusCode >= 500 || resp.StatusCode == 408 {
		return ErrUncertainMutation
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ErrUpstream
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, (2<<20)+1))
	if e != nil || len(b) > 2<<20 {
		return ErrUncertainMutation
	}
	if _, ok := successEnvelope(b); !ok {
		return ErrUncertainMutation
	}
	return nil
}

// Download authenticates only the initial API request. Redirects never inherit credentials.
func (c *Client) Download(ctx context.Context, path string) (*http.Response, error) {
	resp, e := c.downloadStart(ctx, path)
	if e != nil {
		return nil, e
	}
	for n := 0; n < 5 && resp.StatusCode >= 300 && resp.StatusCode < 400; n++ {
		loc, e := resp.Location()
		resp.Body.Close()
		if e != nil || !c.SafeStorageURL(loc.String()) {
			return nil, ErrUpstream
		}
		req, e := http.NewRequestWithContext(ctx, "GET", loc.String(), nil)
		if e != nil {
			return nil, ErrUpstream
		}
		resp, e = c.Storage.Do(req)
		if e != nil {
			return nil, ErrUpstream
		}
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, fmt.Errorf("%w", ErrUpstream)
	}
	return resp, nil
}

// DownloadTarget performs only the authenticated API request and reports how the file
// can be obtained without following any redirect, so the credential never leaves the
// API origin. Exactly one of the two results is set:
//
//   - signed != "": upstream answered with a redirect the browser may fetch itself.
//   - resp != nil (2xx): upstream served the body directly (inline/non-LFS files);
//     the caller owns the body and must close it. This is not an error.
//
// The redirect target must be an allowlisted HTTPS host whose path contains the exact
// hash, so a misconfigured or hostile upstream cannot turn this into an open redirect
// to another object or host. name becomes the download filename: it is not covered by
// the signature, but the value is still sanitized by the caller. Repository layout
// probes (namespace, repository, revision, tag) are stripped; the signature is kept.
func (c *Client) DownloadTarget(ctx context.Context, path, hash, name string) (string, *http.Response, error) {
	resp, e := c.downloadStart(ctx, path)
	if e != nil {
		return "", nil, e
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return "", resp, nil
	}
	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		resp.Body.Close()
		return "", nil, ErrUpstream
	}
	defer resp.Body.Close()
	loc, e := resp.Location()
	if e != nil || !c.SafeStorageURL(loc.String()) || !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(hash) {
		return "", nil, ErrUpstream
	}
	u, e := url.Parse(loc.String())
	// Upstream splits the hash across path segments (/89/6f/a360bd…) and prefixes a
	// bucket path, so compare against the slash-stripped path instead of the raw one.
	if e != nil || !strings.Contains(strings.ReplaceAll(u.Path, "/", ""), hash) {
		return "", nil, ErrUpstream
	}
	q := u.Query()
	for _, k := range []string{"filename", "namespace", "repository", "revision", "tag"} {
		q.Del(k)
	}
	u.RawQuery = q.Encode()
	if u.RawQuery != "" {
		u.RawQuery += "&"
	}
	// filename controls the attachment header on the CDN response and is not covered by
	// the signature, so only values that cannot break out of the quoted header are used;
	// otherwise it falls back to the hash the caller already knows.
	//
	// The value is percent-encoded because raw UTF-8 or spaces make the URL invalid, but
	// upstream echoes whatever it receives into Content-Disposition without decoding it.
	// QueryEscape would turn a space into "+", which the CDN echoes literally (the browser
	// then shows "my+file.apk"), so spaces are re-encoded as %20; browsers decode the rest.
	u.RawQuery += "filename=" + strings.ReplaceAll(url.QueryEscape(safeFilename(name, hash)), "+", "%20")
	return u.String(), nil, nil
}

// safeFilename rejects values that could inject into the upstream Content-Disposition
// header or leak path structure. Store metadata is already validated on upload; this is
// defense in depth against a corrupt or older record.
func safeFilename(name, hash string) string {
	if name == "" || len(name) > 255 || !utf8.ValidString(name) || strings.ContainsAny(name, "\r\n\x00/\\\"") {
		return hash
	}
	return name
}

// downloadStart performs only the authenticated API request without following
// redirects, so the ModelScope credential can never leave the API origin.
func (c *Client) downloadStart(ctx context.Context, path string) (*http.Response, error) {
	q := url.Values{"Revision": {c.C.Revision}, "FilePath": {path}}
	req, e := http.NewRequestWithContext(ctx, "GET", c.path(c.C.RepoType+"/"+c.C.Repo+"/repo")+"?"+q.Encode(), nil)
	if e != nil {
		return nil, ErrUpstream
	}
	c.auth(req)
	resp, e := c.API.Do(req)
	if e != nil {
		return nil, ErrUpstream
	}
	return resp, nil
}

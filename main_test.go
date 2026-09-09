package main

import (
	"FileCabinet/internal/app"
	"FileCabinet/internal/config"
	"FileCabinet/internal/store"
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"io/fs"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEmbeddedFrontendSmoke(t *testing.T) {
	assets, e := fs.Sub(public, "public")
	if e != nil {
		t.Fatal(e)
	}
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	s := store.New(r, "embedded:")
	if e = s.Claim(context.Background()); e != nil {
		t.Fatal(e)
	}
	a := app.New(config.Config{Timeout: time.Minute}, s, nil)
	h := a.Handler(assets)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "/assets/") {
		t.Fatal("embedded frontend missing production bundle")
	}
	if !strings.Contains(w.Header().Get("Content-Security-Policy"), "'wasm-unsafe-eval'") {
		t.Fatal("hash worker CSP missing")
	}
	entries, e := fs.ReadDir(assets, "assets")
	if e != nil {
		t.Fatal(e)
	}
	for _, entry := range entries {
		w = httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/assets/"+entry.Name(), nil))
		if w.Code != 200 || w.Body.Len() == 0 {
			t.Fatal("missing asset", entry.Name())
		}
	}
}

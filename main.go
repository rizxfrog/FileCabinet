package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"FileCabinet/internal/app"
	"FileCabinet/internal/config"
	"FileCabinet/internal/modelscope"
	"FileCabinet/internal/store"
	"github.com/redis/go-redis/v9"
)

//go:embed public
var public embed.FS

func main() {
	if e := run(); e != nil {
		slog.Error("server stopped", "error", e)
		os.Exit(1)
	}
}
func run() error {
	c, e := config.Load()
	if e != nil {
		return e
	}
	opt, e := redis.ParseURL(c.RedisURL)
	if e != nil {
		return errors.New("invalid REDIS_URL")
	}
	opt.DialTimeout = 5 * time.Second
	opt.ReadTimeout = 5 * time.Second
	opt.WriteTimeout = 5 * time.Second
	r := redis.NewClient(opt)
	defer r.Close()
	s := store.New(r, c.Prefix())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	e = s.Claim(ctx)
	cancel()
	if e != nil {
		return fmt.Errorf("cannot claim storage ownership %s; check Redis connectivity and durable owner claim (README recovery)", c.Prefix())
	}
	a := app.New(c, s, modelscope.New(c))
	assets, e := fs.Sub(public, "public")
	if e != nil {
		return e
	}
	life := &app.Lifecycle{}
	server := &http.Server{Addr: c.Addr, Handler: life.Wrap(a.Handler(assets)), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	stop, stopSignal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignal()
	cleanupCtx, stopCleanup := context.WithCancel(context.Background())
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-cleanupCtx.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(cleanupCtx, c.Timeout)
				if e := a.Cleanup(ctx); e != nil {
					slog.Warn("cleanup incomplete; retained for retry")
				}
				cancel()
			}
		}
	}()
	serverErr := make(chan error, 1)
	go func() {
		slog.Info("FileCabinet listening", "addr", c.Addr, "mode", c.UploadMode, "storage_identity", c.Prefix())
		serverErr <- server.ListenAndServe()
	}()
	select {
	case <-stop.Done():
	case e = <-serverErr:
	}
	life.Stop()
	stopCleanup()
	drain, cancel := context.WithTimeout(context.Background(), c.Timeout+15*time.Second)
	shutdownErr := server.Shutdown(drain)
	cancel()
	if shutdownErr != nil {
		a.Uncertain.Store(true)
		_ = server.Close()
	}
	// Do not release ownership just because Shutdown timed out. Wait for every admitted handler.
	life.Wait()
	<-cleanupDone
	if !a.Uncertain.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		releaseErr := s.Release(ctx)
		cancel()
		if releaseErr != nil {
			slog.Warn("durable owner retained: pending mutations or Redis unavailable; see recovery instructions")
		}
	} else {
		slog.Warn("durable owner retained after uncertain I/O; manual reconciliation required")
	}
	if e != nil && !errors.Is(e, http.ErrServerClosed) {
		return errors.New("HTTP listener failed")
	}
	return shutdownErr
}

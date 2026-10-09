package httpserver

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// StartDebugListener deliberately uses a separate, unexposed loopback socket.
// Never mount these handlers on the public mux (Nginx itself connects on loopback).
func StartDebugListener(ctx context.Context) {
	listener, err := net.Listen("tcp", "127.0.0.1:6060")
	if err != nil {
		slog.Warn("loopback profiling listener unavailable")
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	go func() { <-ctx.Done(); _ = server.Close() }()
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Warn("loopback profiling listener stopped")
		}
	}()
}

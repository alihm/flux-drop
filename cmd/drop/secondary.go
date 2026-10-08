package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/runonflux/flux-drop/internal/cluster"
	"github.com/runonflux/flux-drop/internal/storagepool"
)

func runSecondary(c storagepool.Config) error {
	if os.Getenv(cluster.PassphraseEnv) != "" {
		return errors.New("secondary must not have a primary cluster passphrase")
	}
	storage, err := storagepool.NewSecondary(c, "/data", cluster.StateDirectory)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	discoveryCtx, cancelDiscovery := context.WithCancel(ctx)
	discoveryDone := make(chan struct{})
	go func() { defer close(discoveryDone); storage.RunDiscovery(discoveryCtx) }()
	defer func() { cancelDiscovery(); <-discoveryDone; storage.Close() }()
	privateDone, err := storage.Listen(ctx)
	if err != nil {
		return err
	}
	defer func() { stop(); <-privateDone }()
	// Public Nginx has no secondary UI, auth, upload, file-serving or private API.
	public := &http.Server{Addr: "127.0.0.1:8081", Handler: storage.PublicHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	defer public.Close()
	done := make(chan error, 1)
	go func() { done <- public.ListenAndServe() }()
	select {
	case err := <-privateDone:
		return err
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := public.Shutdown(shutdown)
		<-privateDone
		return err
	}
}

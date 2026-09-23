// drop-cluster runs authenticated discovery, consensus, metadata and membership
// services. Its private listeners must never be mounted on public Nginx routes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/runonflux/flux-drop/internal/cluster"
)

func main() {
	createCA := flag.String("create-ca", "", "create a new private CA bundle offline at this path")
	app := flag.String("app", "", "application name for CA creation")
	clusterID := flag.String("cluster-id", "", "32-character lowercase hexadecimal cluster ID for CA creation")
	configFile := flag.String("config", cluster.ManifestPath, "node-local cluster manifest")
	healthcheck := flag.Bool("healthcheck", false, "check the local authenticated coordinator listener without starting Raft")
	showStatus := flag.Bool("status", false, "print authenticated local coordinator status without starting Raft")
	flag.Parse()
	if *createCA != "" {
		bundle, err := cluster.CreateCABundle(*app, *clusterID)
		if err == nil {
			err = cluster.WriteCABundle(*createCA, bundle)
		}
		if err != nil {
			slog.Error("private cluster CA creation failed")
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *healthcheck || *showStatus {
		c, err := cluster.LoadRuntimeConfig(*configFile)
		if err == nil {
			var status cluster.Status
			status, err = cluster.LocalStatus(ctx, c)
			if err == nil && *showStatus {
				err = json.NewEncoder(os.Stdout).Encode(status)
			}
		}
		if err != nil {
			slog.Error("coordinator health check failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if err := run(ctx, *configFile); err != nil {
		slog.Error("cluster coordinator stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, path string) error {
	if path == "" {
		return errors.New("-config is required; discovery never bootstraps a voting configuration")
	}
	c, err := cluster.LoadRuntimeConfig(path)
	if err != nil {
		return err
	}
	runtime, err := cluster.StartRuntime(ctx, c)
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := runtime.Close(shutdown); err != nil {
			slog.Error("cluster shutdown", "error", err)
		}
	}()
	slog.Info("cluster coordinator started", "node", c.Local.ID, "cluster", c.ClusterID, "raft", c.Listen, "status", c.StatusListen)
	select {
	case <-ctx.Done():
		return nil
	case err := <-runtime.Errors():
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("cluster status listener failed: %w", err)
	}
}

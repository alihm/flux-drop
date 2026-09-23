// drop-init supervises the app, Nginx and, when provisioned, the coordinator.
// Any unexpected child exit stops the unit so Flux/Docker can restart it.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/runonflux/flux-drop/internal/cluster"
)

const clusterManifest = cluster.ManifestPath

// An absent manifest is allowed by the supervisor for isolated emulator tests;
// the production app independently requires it. Invalid provisioning fails closed.
func coordinatorConfigured(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, errors.New("coordinator manifest must be a regular file")
	}
	return true, nil
}

func childCommands(coordinator bool) []*exec.Cmd {
	children := []*exec.Cmd{exec.Command("/usr/local/bin/drop"), exec.Command("nginx", "-g", "daemon off;")}
	if coordinator {
		children = append(children, exec.Command("/usr/local/bin/drop-cluster", "-config", clusterManifest))
	}
	return children
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := healthcheck(); err != nil {
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "container stopped:", err)
		os.Exit(1)
	}
}

func healthcheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:8080/healthz", nil)
	if err != nil {
		return err
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("app health check failed")
	}
	configured, err := coordinatorConfigured(clusterManifest)
	if err != nil || !configured {
		return err
	}
	c, err := cluster.LoadRuntimeConfig(clusterManifest)
	if err != nil {
		return err
	}
	return cluster.CheckHealth(ctx, c)
}

func run(ctx context.Context) error {
	if os.Getenv(cluster.PassphraseEnv) != "" {
		if err := cluster.ProvisionAutomatic(ctx, os.Getenv); err != nil {
			return err
		}
	}
	if root := os.Getenv("DROP_DATA_DIR"); root != "" && root != "/data" {
		return errors.New("container Nginx configuration requires DROP_DATA_DIR=/data")
	}
	configured, err := coordinatorConfigured(clusterManifest)
	if err != nil {
		return err
	}
	if configured {
		c, err := cluster.LoadRuntimeConfig(clusterManifest)
		if err != nil {
			return err
		}
		if err := cluster.EnsureCertificate(c); err != nil {
			return err
		}
	}
	children := childCommands(configured)
	exits := make(chan error, len(children))
	started := 0
	shutdown := func() {
		for i := 0; i < started; i++ {
			signal := syscall.SIGTERM
			if i == 1 {
				signal = syscall.SIGQUIT
			}
			_ = children[i].Process.Signal(signal)
		}
	}
	for i, child := range children {
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if i == 1 {
			child.Env = []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
		}
		if err := child.Start(); err != nil {
			shutdown()
			for j := 0; j < started; j++ {
				_ = children[j].Process.Kill()
				<-exits
			}
			return err
		}
		started++
		go func(name string, c *exec.Cmd) { err := c.Wait(); exits <- fmt.Errorf("%s exited: %v", name, err) }(child.Path, child)
	}
	var result error
	remaining := started
	select {
	case <-ctx.Done():
	case result = <-exits:
		remaining--
	}
	shutdown()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for remaining > 0 {
		select {
		case <-exits:
			remaining--
		case <-timer.C:
			for _, c := range children {
				_ = c.Process.Kill()
			}
			for remaining > 0 {
				<-exits
				remaining--
			}
			if result == nil {
				result = errors.New("shutdown deadline exceeded")
			}
		}
	}
	return result
}

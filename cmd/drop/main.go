package main

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	firebase "firebase.google.com/go/v4"

	"github.com/runonflux/flux-drop/internal/cluster"
	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/firebaseconfig"
	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/replica"
	"github.com/runonflux/flux-drop/internal/session"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	discoveryCtx, cancelDiscovery := context.WithTimeout(context.Background(), 60*time.Second)
	get, err := runtimeEnvironment(discoveryCtx, os.Getenv, hostInfoAppName)
	cancelDiscovery()
	if err != nil {
		return err
	}
	legacy, err := legacyMetadata(get)
	if err != nil {
		return err
	}
	publishing, err := publishingFromEnv(get)
	if err != nil {
		return err
	}
	if publishing.enabled {
		if err := validateStorage(publishing, os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")); err != nil {
			return err
		}
		if err := httpserver.StorageReady(publishing.root, content.DefaultLimits()); err != nil {
			return fmt.Errorf("storage not ready: %w", err)
		}
	}
	projectID := firebaseconfig.ProjectFromEnv(os.Getenv)
	if err := session.ValidateEnvironment(get("DROP_ENV"), projectID, os.Getenv("FIRESTORE_EMULATOR_HOST"), os.Getenv("FIREBASE_AUTH_EMULATOR_HOST")); err != nil {
		return err
	}
	dependencies := httpserver.Dependencies{}
	webConfig, err := httpserver.FirebaseWebFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	dependencies.FirebaseWeb = webConfig
	peerConfig, err := replica.RuntimeConfigFromEnv(get)
	if err != nil {
		return err
	}
	if peerConfig != nil && projectID == "" {
		return errors.New("peer runtime requires FIREBASE_PROJECT_ID")
	}
	var projects project.Repository
	maintenanceEnabled := get("DROP_MAINTENANCE_ENABLED")
	if maintenanceEnabled != "" && maintenanceEnabled != "true" && maintenanceEnabled != "false" {
		return errors.New("DROP_MAINTENANCE_ENABLED must be true or false")
	}
	if maintenanceEnabled == "true" && projectID == "" {
		return errors.New("maintenance requires FIREBASE_PROJECT_ID")
	}
	var maintenance interface{ RunMaintenance(context.Context) }
	if !legacy {
		c, err := cluster.LoadRuntimeConfig(cluster.ManifestPath)
		if err != nil {
			return fmt.Errorf("Raft metadata requires provisioned cluster configuration: %w", err)
		}
		if c.ContentDir != publishing.root {
			return errors.New("coordinator and publisher content roots must match")
		}
		if c.Automatic && get("DROP_PEERS_ENABLED") != "false" {
			peerConfig = &replica.RuntimeConfig{App: c.App, Instance: c.Local.ID, DataRoot: c.ContentDir, Certificate: c.Certificate, Key: c.Key, CA: c.CA, Port: cluster.AutoContentPort, ListenPort: cluster.AutoContentPort, Self: c.SelfIPs}
		}
		client, err := cluster.NewClient(c)
		if err != nil {
			return err
		}
		defer client.Close()
		verifier, err := session.NewPublicFirebaseVerifier(context.Background(), projectID)
		if err != nil {
			return err
		}
		budget := int64(60)
		if raw := get("DROP_SESSION_CREATIONS_PER_MINUTE"); raw != "" {
			budget, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || budget < 1 || budget > 10000 {
				return errors.New("invalid DROP_SESSION_CREATIONS_PER_MINUTE")
			}
		}
		store := &metadata.Store{Backend: client}
		dependencies.Sessions = &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: budget}, Verifier: verifier}
		raftProjects := &project.RaftRepository{Store: store, AnonymousByteLimit: publishing.anonymousBytes, AccountByteLimit: publishing.accountBytes}
		projects = raftProjects
		if maintenanceEnabled == "true" {
			maintenance = raftProjects
		}
		if publishing.enabled {
			dependencies.Projects = &project.Publisher{Repository: projects, DataRoot: publishing.root}
			dependencies.Readiness = func(ctx context.Context) error {
				if err := httpserver.StorageReady(publishing.root, content.DefaultLimits()); err != nil {
					return err
				}
				_, err := client.Read(ctx, []string{"health/readiness"})
				return err
			}
		}
	} else if projectID != "" {
		budget := int64(60)
		if raw := os.Getenv("DROP_SESSION_CREATIONS_PER_MINUTE"); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || value < 1 || value > 10000 {
				return errors.New("invalid DROP_SESSION_CREATIONS_PER_MINUTE")
			}
			budget = value
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: projectID})
		if err != nil {
			return err
		}
		store, err := app.Firestore(ctx)
		if err != nil {
			return err
		}
		defer store.Close()
		auth, err := app.Auth(ctx)
		if err != nil {
			return err
		}
		dependencies.Sessions = &session.Service{Store: &session.FirestoreStore{Client: store, CreationsPerMinute: budget}, Verifier: &session.FirebaseVerifier{Client: auth}}
		legacyProjects := &project.FirestoreRepository{Client: store, AnonymousByteLimit: publishing.anonymousBytes, AccountByteLimit: publishing.accountBytes}
		projects = legacyProjects
		if publishing.enabled {
			dependencies.Projects = &project.Publisher{Repository: projects, DataRoot: publishing.root}
			dependencies.Readiness = httpserver.CachedReadiness(func(ctx context.Context) error {
				if err := httpserver.StorageReady(publishing.root, content.DefaultLimits()); err != nil {
					return err
				}
				_, err := store.Collection("drop_projects").Doc("readiness-probe").Get(ctx)
				if status.Code(err) == codes.NotFound {
					return nil
				}
				return err
			})
		}
		if maintenanceEnabled == "true" {
			maintenance = legacyProjects
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var peerErrors <-chan error
	if peerConfig != nil {
		if publishing.enabled && peerConfig.DataRoot != publishing.root {
			return errors.New("peer and publisher data roots must match")
		}
		peers, err := replica.StartRuntime(ctx, *peerConfig, projects)
		if err != nil {
			return err
		}
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := peers.Close(shutdown); err != nil {
				slog.Error("peer shutdown failed", "error", err)
			}
		}()
		dependencies.Fallback = peers.Fallback
		peerErrors = peers.Errors()
	}
	handler, err := httpserver.NewWithDependencies(httpserver.Config{PublicOrigin: get("DROP_PUBLIC_ORIGIN"), Limits: content.DefaultLimits()}, dependencies)
	if err != nil {
		return err
	}
	if publishing.enabled && publishing.password != "" {
		handler = httpserver.StagingAccess(handler, publishing.user, publishing.password)
	}
	server := &http.Server{Addr: "127.0.0.1:8081", Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 60 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	defer server.Close()
	if maintenance != nil {
		workerContext, cancelWorker := context.WithCancel(ctx)
		workerDone := make(chan struct{})
		go func() {
			defer close(workerDone)
			maintenance.RunMaintenance(workerContext)
		}()
		defer func() { cancelWorker(); <-workerDone }()
	}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	slog.Info("application listening", "address", server.Addr, "publishing", publishing.enabled)
	select {
	case err := <-peerErrors:
		return fmt.Errorf("peer listener stopped: %w", err)
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

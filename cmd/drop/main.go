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
	"syscall"
	"time"

	firebase "firebase.google.com/go/v4"

	"github.com/runonflux/flux-drop/internal/admin"
	"github.com/runonflux/flux-drop/internal/cluster"
	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/firebaseconfig"
	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/preview"
	"github.com/runonflux/flux-drop/internal/project"
	"github.com/runonflux/flux-drop/internal/replica"
	"github.com/runonflux/flux-drop/internal/session"
	"github.com/runonflux/flux-drop/internal/storagepool"
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
	storageConfig, err := storagepool.FromEnv(get)
	if err != nil {
		return err
	}
	if storageConfig.Role == "secondary" {
		return runSecondary(storageConfig)
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
		if err := validateStorage(publishing); err != nil {
			return err
		}
		if err := httpserver.StorageReady(publishing.root, cluster.StateDirectory+"/staging", content.DefaultLimits()); err != nil {
			return fmt.Errorf("storage not ready: %w", err)
		}
	}
	var storage *storagepool.Pool
	var storageDone chan struct{}
	var stopStorage context.CancelFunc
	if storageConfig.Role == "primary" {
		if legacy || !publishing.enabled || len(get("DROP_CLUSTER_PASSPHRASE")) < 32 {
			return errors.New("primary storage role requires automatic Raft and publishing")
		}
		storage, err = storagepool.NewPool(storageConfig, cluster.StateDirectory+"/storage-cache")
		if err != nil {
			return err
		}
		workerCtx, cancel := context.WithCancel(context.Background())
		stopStorage = cancel
		storageDone = make(chan struct{})
		go func() { defer close(storageDone); storage.Run(workerCtx) }()
		defer func() { stopStorage(); <-storageDone; storage.Close() }()
	}
	projectID := firebaseconfig.ProjectFromEnv(os.Getenv)
	if err := session.ValidateEnvironment(get("DROP_ENV"), projectID, os.Getenv("FIRESTORE_EMULATOR_HOST"), os.Getenv("FIREBASE_AUTH_EMULATOR_HOST")); err != nil {
		return err
	}
	dependencies := httpserver.Dependencies{}
	dependencies.ProjectBearerVerifier = &session.AgentFirebaseVerifier{ProjectID: projectID}
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
		if storage == nil && c.Automatic && get("DROP_PEERS_ENABLED") != "false" {
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
		budget, err := sessionCreationBudget(get)
		if err != nil {
			return err
		}
		store := &metadata.Store{Backend: client}
		dependencies.Sessions = &session.Service{Store: &session.RaftStore{Store: store, CreationsPerMinute: budget}, Verifier: verifier}
		raftProjects := &project.RaftRepository{Store: store, AnonymousByteLimit: publishing.anonymousBytes, AccountByteLimit: publishing.accountBytes}
		if storage != nil {
			storage.BindMetadata(store)
			adminService, err := admin.New(store, get("DROP_PUBLIC_ORIGIN"), get("DROP_ADMIN_ZELID"))
			if err != nil {
				return err
			}
			dependencies.Admin = adminService.Handler(admin.Source{Snapshot: storage.Dashboard, Change: func(tx *metadata.Tx, name, action string) error {
				err := storage.ChangeApp(tx, name, action)
				if errors.Is(err, storagepool.ErrAppInUse) {
					return admin.ErrAppInUse
				}
				if errors.Is(err, storagepool.ErrUnknownApp) {
					return admin.ErrUnknownApp
				}
				return err
			}})
			raftProjects.StorageOffers = storage.Offers
			dependencies.StorageStatus = storage.AdminHandlerWithOperations(store)
		}
		agentConfig, err := httpserver.AgentAuthFromEnv(get)
		if err != nil {
			return err
		}
		if webConfig != nil {
			dependencies.AgentAuth, err = httpserver.NewAgentAuth(context.Background(), agentConfig, get("DROP_PUBLIC_ORIGIN"), get("DROP_CLUSTER_PASSPHRASE"), raftProjects, webConfig)
			if err != nil {
				return err
			}
		}
		projects = raftProjects
		if maintenanceEnabled == "true" {
			maintenance = raftProjects
		}
		if publishing.enabled {
			dependencies.Projects = &project.Publisher{Repository: projects, DataRoot: publishing.root}
			if storage != nil {
				dependencies.Projects.Installer = storage
			}
			dependencies.StagingRoot = cluster.StateDirectory + "/staging"
			storageReady := httpserver.CachedReadiness(func(context.Context) error {
				return httpserver.StorageReady(publishing.root, dependencies.StagingRoot, content.DefaultLimits())
			})
			dependencies.Readiness = func(ctx context.Context) error {
				if storage != nil && len(storage.Offers()) == 0 {
					return project.ErrStorage
				}
				if err := storageReady(ctx); err != nil {
					return err
				}
				_, err := client.Read(ctx, []string{"health/readiness"})
				return err
			}
		}
	} else if projectID != "" {
		budget, err := sessionCreationBudget(get)
		if err != nil {
			return err
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
			dependencies.StagingRoot = cluster.StateDirectory + "/staging"
			dependencies.Readiness = httpserver.CachedReadiness(func(ctx context.Context) error {
				if err := httpserver.StorageReady(publishing.root, cluster.StateDirectory+"/staging", content.DefaultLimits()); err != nil {
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
	if dependencies.AgentAuth != nil {
		done := make(chan struct{})
		go func() {
			defer close(done)
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			cursors := map[string]string{}
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := dependencies.AgentAuth.Repository.MaintainAgents(ctx, cursors); err != nil && ctx.Err() == nil {
						slog.Warn("agent metadata cleanup unavailable")
					}
				}
			}
		}()
		defer func() { stop(); <-done }()
	}
	var peerErrors <-chan error
	if storage != nil {
		dependencies.Fallback = storage
		peerConfig = nil
	}
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
	if storage != nil && dependencies.Projects != nil {
		raftProjects := projects.(*project.RaftRepository)
		assets := func(w http.ResponseWriter, r *http.Request, p project.Project, name string) {
			storage.ServeAuthorizedProject(w, r, p, name, func(ctx context.Context) error {
				current, err := projects.Resolve(ctx, p.Slug)
				if err != nil {
					return err
				}
				if current.ActiveDigest != p.ActiveDigest || current.Private != p.Private {
					return project.ErrNotFound
				}
				return nil
			})
		}
		previews, err := preview.New(raftProjects.Store, publishing.root+"/thumbnails", preview.BrowserRenderer(assets))
		if err != nil {
			return err
		}
		dependencies.Previews = previews
		dependencies.Projects.Published = previews.Notify
		previewCtx, cancelPreview := context.WithCancel(ctx)
		previewDone := make(chan struct{})
		go func() { defer close(previewDone); previews.Run(previewCtx) }()
		defer func() { cancelPreview(); <-previewDone }()
	}
	handler, err := httpserver.NewWithDependencies(httpserver.Config{PublicOrigin: get("DROP_PUBLIC_ORIGIN"), Limits: content.DefaultLimits()}, dependencies)
	if err != nil {
		return err
	}
	if publishing.enabled && publishing.password != "" {
		handler = httpserver.StagingAccess(handler, publishing.user, publishing.password)
	}
	httpserver.StartDebugListener(ctx)
	server := &http.Server{Addr: "127.0.0.1:8081", Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 330 * time.Second, WriteTimeout: 10 * time.Minute, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
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

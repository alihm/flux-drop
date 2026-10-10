package storagepool

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/httpserver"
	"github.com/runonflux/flux-drop/internal/replica"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

const apiPrefix = "/internal/storage/v1/"

var projectSlugRE = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,111}[a-z0-9])?$`)
var opBucket = []byte("storage-operations-v1")
var errFull = errors.New("storage capacity unavailable")
var errConflict = errors.New("storage operation conflict")
var errMissing = errors.New("storage operation missing")

type Capacity struct {
	Protocol       int    `json:"protocol,omitempty"`
	InstanceID     string `json:"instanceId"`
	Writable       bool   `json:"writable"`
	AppName        string `json:"appName"`
	CapacityBytes  int64  `json:"capacityBytes"`
	AvailableBytes int64  `json:"availableBytes"`
	ReservedBytes  int64  `json:"reservedBytes"`
	FreeInodes     uint64 `json:"freeInodes"`
	TotalInodes    uint64 `json:"totalInodes"`
	ReservedInodes uint64 `json:"reservedInodes"`
	BlockBytes     int64  `json:"blockBytes"`
	TracksInodes   bool   `json:"tracksInodes"`
	HeadroomBytes  int64  `json:"headroomBytes"`
}
type Operation struct {
	Generation string    `json:"generation,omitempty"`
	ProjectID  string    `json:"projectId"`
	Digest     string    `json:"digest"`
	Slug       string    `json:"slug"`
	Bytes      int64     `json:"bytes"`
	Files      int       `json:"files"`
	ExpiresAt  time.Time `json:"expiresAt"`
	State      string    `json:"state,omitempty"`
}

func (o Operation) valid() bool {
	return (o.Generation == "" || digestRE.MatchString(o.Generation)) && idRE.MatchString(o.ProjectID) && digestRE.MatchString(o.Digest) && o.Bytes >= 0 && o.Bytes <= 200<<20 && o.Files > 0 && o.Files <= 5000 && projectSlugRE.MatchString(o.Slug) && o.State == "" && time.Until(o.ExpiresAt) > 0 && time.Until(o.ExpiresAt) <= 16*time.Minute
}
func (o Operation) same(other Operation) bool {
	return o.Generation == other.Generation && o.ProjectID == other.ProjectID && o.Digest == other.Digest && o.Slug == other.Slug && o.Bytes == other.Bytes && o.Files == other.Files
}

// Secondary receipts and in-flight reservations are node-local. Only immutable
// content is replicated by Flux. The primary is the app-wide allocation authority.
type Secondary struct {
	config        Config
	root, staging string
	db            *bbolt.DB
	primary       peerDiscovery
	mu            sync.Mutex
	reclamationMu sync.Mutex
	projectLocks  [256]sync.RWMutex
	versionLocks  [256]sync.RWMutex
	busy          map[string]bool
	slots         chan struct{}
	stagingDisk   *httpserver.StorageAdmission
	probe         func(string, *unix.Statfs_t) error
	instance      string
	requests      sync.WaitGroup
	closed        bool
}

func NewSecondary(c Config, root, state string) (*Secondary, error) {
	if c.Role != "secondary" {
		return nil, errors.New("secondary role required")
	}
	for _, p := range []string{root, state} {
		info, err := os.Lstat(p)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("storage volumes must be existing real directories")
		}
	}
	if _, err := os.Lstat(filepath.Join(root, "cluster-genesis.json")); !os.IsNotExist(err) {
		return nil, errors.New("secondary cannot use a primary content volume")
	}
	identity := struct {
		Schema       int
		App, Primary string
	}{1, c.AppName, c.PrimaryApp}
	raw, _ := json.Marshal(identity)
	for _, identityRoot := range []string{root, state} {
		identityFile := filepath.Join(identityRoot, "storage-identity.json")
		if existing, err := os.ReadFile(identityFile); err == nil {
			if string(existing) != string(raw) {
				return nil, errors.New("stored secondary identity differs from configuration")
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		} else {
			if err := writeIdentity(identityFile, raw); err != nil {
				return nil, err
			}
		}
	}
	instancePath := filepath.Join(state, "storage-instance-id")
	instance, err := os.ReadFile(instancePath)
	if os.IsNotExist(err) {
		id := make([]byte, 16)
		if _, err = rand.Read(id); err != nil {
			return nil, err
		}
		instance = []byte(hex.EncodeToString(id))
		if err = writeIdentity(instancePath, instance); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if !idRE.Match(instance) {
		return nil, errors.New("invalid persisted storage instance identity")
	}
	staging := filepath.Join(state, "storage-staging")
	if err := os.MkdirAll(staging, 0700); err != nil {
		return nil, err
	}
	db, err := bbolt.Open(filepath.Join(state, "storage.db"), 0600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(opBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(retirementBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	// No uploads are running while the exclusively locked DB is opened. Remove
	// only scratch directories owned by this storage runtime after a crash.
	entries, err := os.ReadDir(staging)
	if err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "request-") {
				if err = os.RemoveAll(filepath.Join(staging, e.Name())); err != nil {
					break
				}
			}
		}
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	// Only IP membership is used; secondaries never dial this discovery port.
	primary, err := replica.NewDiscovery(c.PrimaryApp, 1, nil)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Secondary{config: c, root: root, staging: staging, db: db, primary: primary, busy: map[string]bool{}, slots: make(chan struct{}, 4), stagingDisk: httpserver.NewStorageAdmission(staging), probe: unix.Statfs, instance: string(instance)}, nil
}
func (s *Secondary) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.requests.Wait()
	return s.db.Close()
}
func (s *Secondary) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			w.WriteHeader(503)
			return
		}
		s.requests.Add(1)
		s.mu.Unlock()
		defer s.requests.Done()
		next.ServeHTTP(w, r)
	})
}
func (s *Secondary) RunDiscovery(ctx context.Context) { s.primary.Run(ctx) }
func (s *Secondary) authorized(r *http.Request) bool {
	if r.TLS == nil || len(r.Header.Values("Authorization")) != 1 || len(r.Header.Values("X-Drop-Key-ID")) != 1 {
		return false
	}
	addr, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	peers, fresh := s.primary.Snapshot()
	if !fresh {
		return false
	}
	allowed := false
	for _, p := range peers {
		if p.Addr() == addr.Addr().Unmap() {
			allowed = true
			break
		}
	}
	if !allowed {
		return false
	}
	secret := s.config.Keys[r.Header.Get("X-Drop-Key-ID")]
	if secret == "" {
		return false
	}
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return false
	}
	a, b := sha256.Sum256([]byte(got)), sha256.Sum256([]byte(secret))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}
func jsonReply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func storageError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	code := "storage_unavailable"
	switch {
	case errors.Is(err, errConflict):
		status = 409
		code = "operation_conflict"
	case errors.Is(err, errMissing):
		status = 404
		code = "not_found"
	case errors.Is(err, content.ErrInvalid):
		status = 400
		code = "invalid_content"
	case errors.Is(err, content.ErrLimit):
		status = 413
		code = "content_too_large"
	}
	if status == 503 {
		w.Header().Set("Retry-After", "5")
	}
	jsonReply(w, status, map[string]string{"error": code})
}
func (s *Secondary) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+apiPrefix+"status", func(w http.ResponseWriter, r *http.Request) {
		_, fresh := s.primary.Snapshot()
		jsonReply(w, 200, map[string]any{"appName": s.config.AppName, "instanceId": s.instance, "protocol": 2, "discoveryFresh": fresh, "durability": "local-fsync"})
	})
	mux.HandleFunc("GET "+apiPrefix+"capacity", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		c, err := s.capacity()
		if err != nil {
			storageError(w, err)
			return
		}
		jsonReply(w, 200, c)
	})
	mux.HandleFunc("GET "+apiPrefix+"inventory", s.inventory)
	mux.HandleFunc("GET "+apiPrefix+"replication", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		snapshot, _, err := s.replication(r.Context())
		if err != nil {
			storageError(w, err)
			return
		}
		jsonReply(w, 200, snapshot)
	})
	mux.HandleFunc("POST "+apiPrefix+"reclaim", s.reclaim)
	mux.HandleFunc("GET "+apiPrefix+"generations/{projectId}/{generation}/{digest}/manifest", s.manifest)
	mux.HandleFunc("GET "+apiPrefix+"generations/{projectId}/{generation}/{digest}/files/{path...}", s.file)
	mux.HandleFunc("HEAD "+apiPrefix+"generations/{projectId}/{generation}/{digest}/files/{path...}", s.file)
	mux.HandleFunc("PUT "+apiPrefix+"operations/{operationId}", s.reserve)
	mux.HandleFunc("GET "+apiPrefix+"operations/{operationId}", s.operation)
	mux.HandleFunc("DELETE "+apiPrefix+"operations/{operationId}", s.cancel)
	mux.HandleFunc("PUT "+apiPrefix+"operations/{operationId}/content", s.upload)
	mux.HandleFunc("POST "+apiPrefix+"operations/{operationId}/commit", s.commit)
	mux.HandleFunc("GET "+apiPrefix+"versions/{projectId}/{digest}/manifest", s.manifest)
	mux.HandleFunc("GET "+apiPrefix+"versions/{projectId}/{digest}/files/{path...}", s.file)
	mux.HandleFunc("HEAD "+apiPrefix+"versions/{projectId}/{digest}/files/{path...}", s.file)
	return s.guard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if !s.authorized(r) {
			jsonReply(w, 403, map[string]string{"error": "forbidden"})
			return
		}
		if (r.URL.RawQuery != "" && r.URL.Path != apiPrefix+"inventory") || r.URL.EscapedPath() != (&url.URL{Path: r.URL.Path}).EscapedPath() {
			http.NotFound(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	}))
}
func (s *Secondary) read(id string) (Operation, error) {
	if !digestRE.MatchString(id) {
		return Operation{}, content.ErrInvalid
	}
	var o Operation
	err := s.db.View(func(tx *bbolt.Tx) error {
		raw := tx.Bucket(opBucket).Get([]byte(id))
		if raw == nil {
			return errMissing
		}
		return json.Unmarshal(raw, &o)
	})
	return o, err
}
func (s *Secondary) save(id string, o Operation) error {
	raw, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(opBucket).Put([]byte(id), raw) })
}
func (s *Secondary) capacity() (Capacity, error) {
	var fs unix.Statfs_t
	if err := s.probe(s.root, &fs); err != nil {
		return Capacity{}, err
	}
	writableStorage := writable(s.root) == nil && writable(s.staging) == nil
	if fs.Bsize <= 0 || fs.Bsize > 1<<20 || fs.Bavail > uint64((1<<63-1)/fs.Bsize) {
		return Capacity{}, errFull
	}
	c := Capacity{Protocol: 2, InstanceID: s.instance, Writable: writableStorage, AppName: s.config.AppName, CapacityBytes: s.config.Capacity, AvailableBytes: int64(fs.Bavail) * fs.Bsize, FreeInodes: fs.Ffree, TotalInodes: fs.Files, BlockBytes: fs.Bsize, TracksInodes: fs.Files != 0, HeadroomBytes: Headroom}
	err := s.db.Update(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(opBucket).Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var o Operation
			if json.Unmarshal(v, &o) != nil {
				return errors.New("damaged storage receipt")
			}
			if !time.Now().Before(o.ExpiresAt) && !s.busy[string(k)] {
				if err := cursor.Delete(); err != nil {
					return err
				}
				continue
			}
			if o.State == "reserved" {
				n := uint64(o.Files)*21 + 64
				c.ReservedInodes += n
				c.ReservedBytes += o.Bytes + int64(n)*fs.Bsize + 8<<20
			}
		}
		return nil
	})
	return c, err
}
func (s *Secondary) reserve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("operationId")
	if !digestRE.MatchString(id) {
		storageError(w, content.ErrInvalid)
		return
	}
	var o Operation
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	d.DisallowUnknownFields()
	var extra any
	if d.Decode(&o) != nil || d.Decode(&extra) != io.EOF || !o.valid() || o.Generation != "" && o.Generation != id {
		storageError(w, content.ErrInvalid)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.notRetired(o); err != nil {
		storageError(w, err)
		return
	}
	c, err := s.capacity()
	if err != nil {
		storageError(w, err)
		return
	}
	old, err := s.read(id)
	if err == nil {
		if !old.same(o) || old.State == "canceled" || !time.Now().Before(old.ExpiresAt) {
			storageError(w, errConflict)
			return
		}
		if old.State == "stored" {
			if err := content.SyncVersion(s.operationVersion(old), old.Digest); err != nil {
				storageError(w, err)
				return
			}
		}
		jsonReply(w, 200, old)
		return
	}
	if !errors.Is(err, errMissing) {
		storageError(w, err)
		return
	}
	var count int
	s.db.View(func(tx *bbolt.Tx) error { count = tx.Bucket(opBucket).Stats().KeyN; return nil })
	if count >= 8192 {
		storageError(w, errFull)
		return
	}
	inodes := uint64(o.Files)*21 + 64
	need := o.Bytes + int64(inodes)*c.BlockBytes + 8<<20
	if !c.Writable || c.AvailableBytes < Headroom+c.ReservedBytes+need*2 || (c.TracksInodes && (c.FreeInodes < 1024+c.ReservedInodes+inodes*2)) {
		storageError(w, errFull)
		return
	}
	release, err := s.stagingDisk.Acquire(content.Limits{UploadBytes: MaxBody, ExpandedBytes: 200 << 20, Files: 5000})
	if err != nil {
		storageError(w, err)
		return
	}
	release()
	o.State = "reserved"
	if err := s.save(id, o); err != nil {
		storageError(w, err)
		return
	}
	jsonReply(w, 200, o)
}
func (s *Secondary) operation(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, err := s.read(r.PathValue("operationId"))
	if err != nil {
		storageError(w, err)
		return
	}
	if !time.Now().Before(o.ExpiresAt) {
		storageError(w, errMissing)
		return
	}
	if err = s.notRetired(o); err != nil {
		storageError(w, err)
		return
	}
	if o.State == "stored" {
		if _, err = content.VerifyVersion(s.operationVersion(o), o.Digest); err != nil {
			o.State = "missing"
		}
	}
	jsonReply(w, 200, o)
}
func (s *Secondary) cancel(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := r.PathValue("operationId")
	o, err := s.read(id)
	if err != nil {
		storageError(w, err)
		return
	}
	if s.busy[id] || o.State == "stored" {
		storageError(w, errConflict)
		return
	}
	o.State = "canceled"
	if err = s.save(id, o); err != nil {
		storageError(w, err)
		return
	}
	jsonReply(w, 200, o)
}
func (s *Secondary) commit(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, err := s.read(r.PathValue("operationId"))
	if err != nil {
		storageError(w, err)
		return
	}
	if s.notRetired(o) != nil || o.State != "stored" || !time.Now().Before(o.ExpiresAt) {
		storageError(w, errConflict)
		return
	}
	if err = content.SyncVersion(s.operationVersion(o), o.Digest); err != nil {
		storageError(w, err)
		return
	}
	jsonReply(w, 200, o)
}
func (s *Secondary) upload(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/zip" {
		storageError(w, content.ErrInvalid)
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		storageError(w, errFull)
		return
	}
	id := r.PathValue("operationId")
	s.mu.Lock()
	o, err := s.read(id)
	if err == nil && (o.State != "reserved" && o.State != "stored" || s.busy[id] || !time.Now().Before(o.ExpiresAt)) {
		err = errConflict
	}
	if err == nil {
		err = s.notRetired(o)
	}
	if err == nil {
		s.busy[id] = true
	}
	s.mu.Unlock()
	if err != nil {
		storageError(w, err)
		return
	}
	defer func() { s.mu.Lock(); delete(s.busy, id); s.mu.Unlock() }()
	release, err := s.stagingDisk.Acquire(content.Limits{UploadBytes: MaxBody, ExpandedBytes: 200 << 20, Files: 5000})
	if err != nil {
		storageError(w, err)
		return
	}
	defer release()
	dir, err := os.MkdirTemp(s.staging, "request-")
	if err != nil {
		storageError(w, err)
		return
	}
	defer os.RemoveAll(dir)
	f, err := os.CreateTemp(dir, "body-")
	if err != nil {
		storageError(w, err)
		return
	}
	defer f.Close()
	n, err := io.Copy(f, http.MaxBytesReader(w, r.Body, MaxBody))
	if err != nil {
		storageError(w, content.ErrLimit)
		return
	}
	staged, err := content.StageZIP(dir, f, n, content.Limits{UploadBytes: MaxBody, ExpandedBytes: 200 << 20, Files: 5000})
	if err != nil {
		storageError(w, err)
		return
	}
	defer staged.Discard()
	var bytes int64
	for _, file := range staged.Manifest.Files {
		bytes += file.Size
	}
	if staged.Digest != o.Digest || bytes != o.Bytes || len(staged.Manifest.Files) != o.Files {
		storageError(w, content.ErrInvalid)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !time.Now().Before(o.ExpiresAt) || s.notRetired(o) != nil {
		storageError(w, errConflict)
		return
	}
	if o.Generation != "" {
		err = staged.InstallGeneration(s.root, o.ProjectID, o.Slug, o.Generation)
	} else {
		err = staged.Install(s.root, o.ProjectID, o.Slug)
	}
	if err != nil {
		storageError(w, err)
		return
	}
	o.State = "stored"
	if err = s.save(id, o); err != nil {
		storageError(w, err)
		return
	}
	jsonReply(w, 200, o)
}
func (s *Secondary) operationVersion(o Operation) string {
	if o.Generation == "" {
		return s.version(o.ProjectID, o.Digest)
	}
	relative, _ := content.GenerationPath(o.ProjectID, o.Generation)
	return filepath.Join(s.root, relative)
}
func (s *Secondary) version(id, digest string) string {
	return filepath.Join(s.root, "projects", id, "versions", digest)
}
func (s *Secondary) verified(r *http.Request) (content.Manifest, string, error) {
	id, digest := r.PathValue("projectId"), r.PathValue("digest")
	if !idRE.MatchString(id) || !digestRE.MatchString(digest) {
		return content.Manifest{}, "", content.ErrInvalid
	}
	dir := s.version(id, digest)
	if generation := r.PathValue("generation"); generation != "" {
		if !digestRE.MatchString(generation) {
			return content.Manifest{}, "", content.ErrInvalid
		}
		relative, _ := content.GenerationPath(id, generation)
		dir = filepath.Join(s.root, relative)
	}
	if err := s.notRetired(Operation{ProjectID: id, Digest: digest, Generation: r.PathValue("generation")}); err != nil {
		return content.Manifest{}, "", err
	}
	m, err := content.ServingVersions.VerifyContext(r.Context(), dir, digest, r.PathValue("path"))
	return m, dir, err
}
func (s *Secondary) manifest(w http.ResponseWriter, r *http.Request) {
	unlock := s.lockVersionRead(r.PathValue("projectId"), r.PathValue("digest"), r.PathValue("generation"))
	defer unlock()
	m, _, err := s.verified(r)
	if err != nil {
		storageError(w, err)
		return
	}
	jsonReply(w, 200, m)
}
func (s *Secondary) file(w http.ResponseWriter, r *http.Request) {
	unlock := s.lockVersionRead(r.PathValue("projectId"), r.PathValue("digest"), r.PathValue("generation"))
	defer unlock()
	m, dir, err := s.verified(r)
	if err != nil {
		storageError(w, err)
		return
	}
	name := r.PathValue("path")
	if content.ValidatePath(name) != nil {
		http.NotFound(w, r)
		return
	}
	for _, entry := range m.Files {
		if entry.Path != name {
			continue
		}
		root, err := os.OpenRoot(dir)
		if err != nil {
			storageError(w, err)
			return
		}
		defer root.Close()
		f, err := root.Open("public/" + name)
		if err != nil {
			storageError(w, err)
			return
		}
		defer f.Close()
		// Verify the very descriptor that is served, including changes between the
		// directory verification and open. Syncthing never supplies authorization.
		if err := content.DescriptorHashes.Verify(r.Context(), f, r.PathValue("digest"), entry); err != nil {
			storageError(w, err)
			return
		}

		w.Header().Set("ETag", "\""+entry.SHA256+"\"")
		http.ServeContent(w, r, name, time.Time{}, f)
		return
	}
	http.NotFound(w, r)
}
func (s *Secondary) Listen(ctx context.Context) (<-chan error, error) {
	config, err := serverTLS(s.config)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: s.Handler(), TLSConfig: config, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Minute, WriteTimeout: 5 * time.Minute, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() {
		err := server.ServeTLS(listener, "", "")
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		done <- err
		close(done)
	}()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		_ = server.Close()
	}()
	return done, nil
}

func (s *Secondary) PublicHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { jsonReply(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		c, err := s.capacity()
		s.mu.Unlock()
		_, fresh := s.primary.Snapshot()
		if err != nil || !fresh || !c.Writable || c.AvailableBytes-c.ReservedBytes <= Headroom {
			jsonReply(w, 503, map[string]string{"status": "not_ready"})
			return
		}
		jsonReply(w, 200, map[string]string{"status": "ready"})
	})
	return s.guard(mux)
}

func writable(root string) error {
	f, err := os.CreateTemp(root, ".storage-health-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.WriteString("ready"); err != nil {
		return err
	}
	return f.Sync()
}

func writeIdentity(path string, raw []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".storage-identity-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Link(f.Name(), path); err != nil {
		if !os.IsExist(err) {
			return err
		}
		existing, readErr := os.ReadFile(path)
		if readErr != nil || string(existing) != string(raw) {
			return errors.New("stored secondary identity differs from configuration")
		}
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

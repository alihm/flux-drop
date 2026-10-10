package storagepool

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/runonflux/flux-drop/internal/content"
	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

// Run with DROP_TEST_SYNCTHING_BINARY=/path/to/syncthing-v1.30.0 go test
// -race ./internal/storagepool -run TestSyncthingReplicationReclamation.
// This starts two private local processes; it never touches a deployed app.
func TestSyncthingReplicationReclamation(t *testing.T) {
	binary := os.Getenv("DROP_TEST_SYNCTHING_BINARY")
	if binary == "" {
		t.Skip("set DROP_TEST_SYNCTHING_BINARY to test real replication")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	a, _, pool := fixture(t, t.TempDir(), nil)
	b, bServer, _ := fixture(t, t.TempDir(), nil)
	reservePort := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := l.Addr().String()
		l.Close()
		return address
	}
	start := func(s *Secondary) (string, string, func(), func()) {
		home := t.TempDir()
		gui, syncAddress := reservePort(), reservePort()
		generate := exec.CommandContext(ctx, binary, "generate", "--home", home, "--no-default-folder")
		if raw, err := generate.CombinedOutput(); err != nil {
			t.Fatalf("generate: %v %s", err, raw)
		}
		configPath := filepath.Join(home, "config.xml")
		raw, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatal(err)
		}
		config := strings.ReplaceAll(string(raw), "<listenAddress>default</listenAddress>", "<listenAddress>tcp://"+syncAddress+"</listenAddress>")
		for _, name := range []string{"globalAnnounceEnabled", "localAnnounceEnabled", "relaysEnabled", "natEnabled"} {
			config = strings.ReplaceAll(config, "<"+name+">true</"+name+">", "<"+name+">false</"+name+">")
		}
		if err = os.WriteFile(configPath, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		s.config.SyncthingURL, s.config.SyncthingAPIKey, s.config.SyncthingFolder = "http://"+gui, testKey, "dropdata"
		var command *exec.Cmd
		var log *os.File
		launch := func() {
			log, err = os.CreateTemp(t.TempDir(), "syncthing-*.log")
			if err != nil {
				t.Fatal(err)
			}
			command = exec.CommandContext(ctx, binary, "serve", "--home", home, "--no-browser", "--no-restart", "--no-upgrade", "--gui-address", gui, "--gui-apikey", testKey)
			command.Stdout, command.Stderr = log, log
			// Run the server directly: killing a supervising monitor alone can
			// leave its child holding the database lock during the restart test.
			command.Env = append(os.Environ(), "GOMAXPROCS=2", "STMONITORED=1")
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			for deadline := time.Now().Add(15 * time.Second); ; {
				var status any
				if s.syncRequest(ctx, "GET", "/rest/system/status", nil, &status) == nil {
					break
				}
				if time.Now().After(deadline) {
					data, _ := os.ReadFile(log.Name())
					t.Fatalf("Syncthing did not start: %s", data)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		stop := func() {
			if command != nil {
				command.Process.Kill()
				command.Wait()
				log.Close()
				command = nil
			}
		}
		t.Cleanup(stop)
		launch()
		var status struct{ MyID string }
		if err = s.syncRequest(ctx, "GET", "/rest/system/status", nil, &status); err != nil {
			t.Fatal(err)
		}
		return status.MyID, syncAddress, stop, launch
	}
	aID, aAddress, _, _ := start(a)
	bID, bAddress, stopB, startB := start(b)
	for i, s := range []*Secondary{a, b} {
		otherID, otherAddress := bID, bAddress
		if i == 1 {
			otherID, otherAddress = aID, aAddress
		}
		var device map[string]any
		if err := s.syncRequest(ctx, "GET", "/rest/config/defaults/device", nil, &device); err != nil {
			t.Fatal(err)
		}
		device["deviceID"], device["addresses"] = otherID, []string{"tcp://" + otherAddress}
		if err := s.syncRequest(ctx, "PUT", "/rest/config/devices/"+otherID, device, nil); err != nil {
			t.Fatal(err)
		}
		var folder map[string]any
		if err := s.syncRequest(ctx, "GET", "/rest/config/defaults/folder", nil, &folder); err != nil {
			t.Fatal(err)
		}
		folder["id"], folder["path"], folder["type"], folder["rescanIntervalS"], folder["fsWatcherEnabled"] = "dropdata", s.root, "sendreceive", 1, false
		folder["devices"] = []map[string]string{{"deviceID": aID}, {"deviceID": bID}}
		if err := s.syncRequest(ctx, "PUT", "/rest/config/folders/dropdata", folder, nil); err != nil {
			t.Fatal(err)
		}
	}
	addr := netip.MustParseAddrPort(bServer.Listener.Addr().String())
	discovery := pool.apps[0].discovery.(*testDiscovery)
	peers, _ := discovery.Snapshot()
	discovery.set(append(peers, addr), true)
	pool.refresh(ctx, pool.apps[0])
	repo, actor, active, old := publishTwoGenerations(t, pool)
	for deadline := time.Now().Add(35 * time.Second); ; {
		all := true
		for _, s := range []*Secondary{a, b} {
			if _, err := content.VerifyVersion(s.operationVersion(Operation{ProjectID: old.ProjectID, Digest: old.Digest, Generation: old.Generation}), old.Digest); err != nil {
				all = false
			}
			if _, err := content.VerifyVersion(s.operationVersion(Operation{ProjectID: active.ID, Digest: active.ActiveDigest, Generation: active.StorageGeneration}), active.ActiveDigest); err != nil {
				all = false
			}
		}
		if all {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real Syncthing did not replicate both generations")
		}
		time.Sleep(100 * time.Millisecond)
	}
	before := allocation(t, pool)
	stopB()
	if _, err := pool.ReclaimObsolete(ctx, "storagea", ""); err == nil {
		t.Fatal("offline replica accepted as cleanup proof")
	}
	if allocation(t, pool) != before {
		t.Fatal("offline replica refunded")
	}
	startB()
	if _, err := pool.ReclaimObsolete(ctx, "storagea", ""); err != nil {
		t.Fatal(err)
	}
	if got := allocation(t, pool); got.Bytes != before.Bytes-old.Bytes || got.Versions != 1 {
		t.Fatal(got)
	}
	// Give the resumed scanners/pullers several full scan cycles to process their
	// old indexes. The permanent ignores must prevent resurrection on both nodes.
	for range 25 {
		for _, s := range []*Secondary{a, b} {
			if _, err := os.Stat(s.operationVersion(Operation{ProjectID: old.ProjectID, Digest: old.Digest, Generation: old.Generation})); !os.IsNotExist(err) {
				t.Fatal("retired version reappeared", err)
			}
			if _, err := content.VerifyVersion(s.operationVersion(Operation{ProjectID: active.ID, Digest: active.ActiveDigest, Generation: active.StorageGeneration}), active.ActiveDigest); err != nil {
				t.Fatal("active generation damaged", err)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Tombstoning the project also removes its shared marker and parent layout;
	// otherwise refunding the last version would leave unaccounted directories.
	if err := repo.Tombstone(ctx, actor, active.ID, active.Revision); err != nil {
		t.Fatal(err)
	}
	if err := pool.store.Run(ctx, func(tx *metadata.Tx) error {
		key := "storage_versions/" + project.StorageVersionID(active.ID, active.ActiveDigest, active.StorageGeneration)
		var version project.StorageVersion
		if err := tx.Get(key, &version); err != nil {
			return err
		}
		version.CreatedAt = time.Now().Add(-2 * time.Hour)
		return tx.Set(key, version)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.ReclaimObsolete(ctx, "storagea", ""); err != nil {
		t.Fatal(err)
	}
	if got := allocation(t, pool); got.Bytes != 0 || got.Inodes != 0 || got.Versions != 0 {
		t.Fatal(got)
	}
	for _, s := range []*Secondary{a, b} {
		if _, err := os.Stat(filepath.Join(s.root, "projects", active.ID)); !os.IsNotExist(err) {
			t.Fatal("deleted project namespace retained", err)
		}
	}
	t.Log(fmt.Sprintf("two Syncthing v1.30.0 members acknowledged generation %s", old.Generation))
}

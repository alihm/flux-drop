package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRequireAutomaticMounts(t *testing.T) {
	const root = "1 0 0:1 / / rw - overlay overlay rw\n"
	const data = "2 1 7:2 /appdata /data rw - ext4 /dev/loop2 rw\n"
	const state = "3 1 7:3 /state /var/lib/drop-cluster rw - ext4 /dev/loop3 rw\n"
	for _, tc := range []struct {
		name  string
		table string
		want  string
	}{
		{"separate mounts", root + data + state, ""},
		{"incident missing local mount", root + data, "/var/lib/drop-cluster is not a dedicated mount"},
		{"missing content mount", root + state, "/data is not a dedicated mount"},
		{"invalid mount table", "broken\n", "invalid mount table"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "mountinfo")
			if err := os.WriteFile(path, []byte(tc.table), 0600); err != nil {
				t.Fatal(err)
			}
			err := requireAutomaticMounts("/var/lib/drop-cluster", "/data", path)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("requireAutomaticMounts() = %v, want %q", err, tc.want)
			}
		})
	}
}

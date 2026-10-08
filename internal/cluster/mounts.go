package cluster

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// requireAutomaticMounts checks only what the container can prove: both data
// roots are dedicated mounts. Flux's replication policy and whether a mount
// survives a replacement must still be configured and tested by the operator.
func requireAutomaticMounts(stateDir, contentDir, mountinfo string) error {
	f, err := os.Open(mountinfo)
	if err != nil {
		return fmt.Errorf("cannot verify persistent volumes: %w", err)
	}
	defer f.Close()

	var stateID, contentID string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			return errors.New("invalid mount table; cannot verify persistent volumes")
		}
		point := decodeMountPoint(fields[4])
		switch point {
		case stateDir:
			stateID = fields[0]
		case contentDir:
			contentID = fields[0]
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("cannot read mount table: %w", err)
	}
	if stateID == "" {
		return fmt.Errorf("%s is not a dedicated mount; set Flux Container Data to r:/data|ml:state:/var/lib/drop-cluster before starting Drop", stateDir)
	}
	if contentID == "" {
		return fmt.Errorf("%s is not a dedicated mount; configure the replicated Flux volume there before starting Drop", contentDir)
	}
	if stateID == contentID || filepath.Clean(stateDir) == filepath.Clean(contentDir) {
		return errors.New("cluster state and content must use separate mounts")
	}
	return nil
}

// mountinfo escapes spaces and a few other bytes in mount paths.
func decodeMountPoint(path string) string {
	r := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return r.Replace(path)
}

// PrepareStorageVolumes applies the same persistent local-state boundary to a
// secondary, which has no metadata coordinator or coordinator credentials.
func PrepareStorageVolumes() error {
	if err := requireAutomaticMounts(StateMountDirectory, "/data", "/proc/self/mountinfo"); err != nil {
		return err
	}
	return prepareAutomaticStateDirectory(StateMountDirectory, StateDirectory)
}

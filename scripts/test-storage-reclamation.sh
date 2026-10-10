#!/usr/bin/env bash
set -euo pipefail

# Extract the audited, pinned release without installing a host service.
storage_test_dir=$(mktemp -d)
storage_test_container=""
storage_test_cleanup() {
  if [[ -n "$storage_test_container" ]]; then
    docker rm "$storage_test_container" >/dev/null
  fi
  rm -rf -- "$storage_test_dir"
}
trap storage_test_cleanup EXIT
storage_test_container=$(docker create syncthing/syncthing:1.30.0@sha256:74eeedb08d4912763055594f8bd98bfc039f3bc504b6cd2c2adc8294111c1251)
docker cp "$storage_test_container:/bin/syncthing" "$storage_test_dir/syncthing"
chmod +x "$storage_test_dir/syncthing"
DROP_TEST_SYNCTHING_BINARY="$storage_test_dir/syncthing" go test -race ./internal/storagepool -run '^TestSyncthingReplicationReclamation$' -count=1 -v

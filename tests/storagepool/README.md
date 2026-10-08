# Storage pool verification

The real TLS transfer, browser lifecycle, accounting, replication-readiness and
cache/access tests run in `go test -race ./internal/storagepool ./internal/project`.
Their explicitly private discovery test doubles cannot be selected in production.

Build the final image and exercise both production roles with isolated volumes:

```sh
docker build -t flux-drop:storage-pool .
node tests/storagepool/smoke.mjs
```

Use `node tests/storagepool/smoke.mjs --image alihmahdavi/flux-drop:staging` to verify a different local image. The smoke checks include the primary admin page, anonymous dashboard denial, and absence of admin endpoints on secondaries.

The smoke runner creates a unique Compose project, uses `network_mode: none`, and
removes only that project's disposable containers/volumes afterward. It checks
both roles' actual supervisor/Nginx/Go startup and liveness, secondary public API
isolation, fail-closed readiness without discovery/quorum, refusal to bootstrap a
primary without discovery, non-root private state modes, and secondary identity
persistence across restart. It makes no production requests and does not simulate
actual Syncthing. Live Flux networking and replication remain acceptance checks.

# Flux Drop

Static publishing on Flux with anonymous ownership, Google claiming, and
replicated content. Live Flux and Google sign-in acceptance is still required.

## Deploy for testing

The image includes Go, Nginx, process supervision and health checks. Automatic
Flux deployment uses one private passphrase and two persistent volumes. See the
[Flux deployment guide](docs/FLUX_DEPLOYMENT.md). A separate staging password is
optional for a restricted test site.

```sh
docker build --pull -t YOUR_REGISTRY/flux-drop:staging-1 .
docker compose -f tests/staging/compose.yaml up --build -d
node tests/staging/run.mjs
docker compose -f tests/staging/compose.yaml down -v
```

The test stack uses isolated Firebase emulators and disposable test storage, never
production credentials. The lifecycle test covers the final image through Nginx,
including authentication/claiming, privacy, restart persistence, readiness failures
and supervisor recovery. Real Google popup and Flux networking/replication still
require acceptance on the deployed hostname.

## Implemented

- HTML/ZIP/folder ingestion, canonical manifests and immutable digest-based versions.
- Session-owned project listing, rename, stable-URL replacement, delete, Google claim.
- Secure session/CSRF rotation and Raft transaction-backed ownership and quotas.
- Password-protected self-contained HTML, with revocable session-bound access grants.
- Opaque-origin sandboxed Nginx delivery and authenticated one-hop public peer fallback.
- Bounded maintenance, disk/inode admission and conservative retained-byte budgets.
- Landing/upload UI, project cards and management dialog, and a locally bundled
  memory-only Google popup client. See [the UI guide](docs/FRONTEND.md).
- Read-only retention audit, internal retirement fences and a generation installer
  foundation. Automated retirement/deletion and generation-layout publication are
  **not enabled**; retained bytes are not refunded without verified reclamation.

## Runtime

Automatic deployment uses one private `DROP_CLUSTER_PASSPHRASE`, two volumes and
no per-node manifests. See [Flux deployment](docs/FLUX_DEPLOYMENT.md) for port
mappings, bootstrap assumptions and remaining live acceptance tests. One image
supervises the app, Nginx and coordinator. Raft handles coordination/security;
content writes use local-durable asynchronous acknowledgements. Firebase remains
auth-only without service-account credentials.

Go listens only on 127.0.0.1:8081; Nginx exposes port 8080. TLS must terminate at
trusted public ingress. `/healthz` is liveness; `/readyz` checks configured
publishing, local storage and current metadata authority. Publishing defaults to enabled;
set `DROP_PUBLISHING_ENABLED=false` to disable it (readiness then returns 503).

On Flux, public settings need no environment configuration: the app name is taken
from `FLUX_APP_NAME`, `APP_NAME`, or the local Flux hostinfo service to derive the
HTTPS origin. Orbit's public Firebase settings are bundled. The shared private
passphrase provisions cluster identity automatically. An independent staging
password is optional in automatic mode. Firebase
service-account credentials are rejected by the new runtime.
Use a dedicated Firebase test project with explicit overrides for isolated testing.
Cluster admission credentials and peer keys
must remain outside images, public Flux environment parameters and replicated
`/data`. The container runs as UID/GID 65534. See [.env.example](.env.example).
Staging Basic access gates management/upload/unlock, while public project assets
remain public for sandbox compatibility.

## Verify

```sh
go test -race ./...
go vet ./...
go build -o bin/drop ./cmd/drop
```

The image builds with Go 1.27.1 and Nginx 1.30.5. The module retains Go 1.25 as its
minimum language version. Additional suites:

- [Final-image staging lifecycle](tests/staging/README.md)
- [Browser isolation and UI](tests/browser/README.md)
- [Production-config Nginx delivery](tests/delivery/README.md)
- [Firestore transactions](tests/firestore/README.md)

## Limitations and design

Private hosting currently supports self-contained HTML only, with no private
replica fallback. Public prebuilt React/Vue bundles work within the
[static compatibility restrictions](docs/STATIC_COMPATIBILITY.md).

See [storage budgets](docs/STORAGE.md),
[reclamation requirements](docs/RECLAMATION.md), [session APIs](docs/SESSIONS.md),
[publishing APIs](docs/PUBLISHING.md), [frontend configuration](docs/FRONTEND.md),
and [replica internals](docs/REPLICAS.md). Historical progress notes in those files
are superseded by the staging runtime and deployment guides above.

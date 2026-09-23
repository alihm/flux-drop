# Build and deploy a restricted Flux test

For the current one-secret setup, use [FLUX_DEPLOYMENT.md](FLUX_DEPLOYMENT.md).
Manual CA/enrollment instructions below describe the older compatibility path.
Add a separate optional staging password to restrict the test UI.

The source runtime now uses Raft for metadata and Firebase only for Google token
verification. It **rejects Firebase service-account credentials**. Previously
published Docker images are unchanged; do not apply these settings to an old tag.

This remains a restricted staging build, not a finished production rollout.

## Current configuration

Supply privately:

```text
DROP_STAGING_PASSWORD=<at-least-20-random-characters>
DROP_CLUSTER_CA_BUNDLE_B64=<base64-private-cluster-CA-JSON>
```

The CA bundle enables automatic certificate provisioning/renewal and derives the
membership admission credential. Every participating node needs the same bundle.
Use private Flux configuration, never public environment parameters. Alternatively
put the bundle in a mode-0600 file and set `caBundleFile` in the coordinator
manifest; do not configure both sources. No Firebase private credentials are needed.

The runtime also currently needs:

- `/run/secrets/cluster-node.json`: node-specific coordinator manifest.
- A privately supplied shared cluster CA bundle (managed TLS), or manually
  provisioned unique node certificates and the public CA bundle.
- An explicit initial manifest of three or five voters for first bootstrap.

See [CLUSTER.md](CLUSTER.md). Managed TLS generates and renews certificates with
node-local keys. A separate enrollment key does not elect a leader,
or authorize forming a new cluster after quorum loss.

Do not set `GOOGLE_APPLICATION_CREDENTIALS` or
`DROP_FIREBASE_CREDENTIALS_B64`. Public Firebase settings default to Orbit's
configuration and may be overridden for a separate test project. Google sign-in
and the exact app hostname must be enabled/authorized in that Firebase project.
No Firestore database, indexes or service-account permissions are required by the
new production path.

The public origin defaults from Flux app-name/hostinfo discovery. Data defaults
to `/data`, publishing to enabled, staging username to `tester`, and production
mode to enabled. `DROP_PUBLIC_ORIGIN` is only needed for a custom origin.

## One image, one Flux component

- Public port: 8080 behind HTTPS ingress. Never expose app loopback port 8081.
- Raft port: 8445; authenticated status/metadata/enrollment port: 8446.
- Use the same external status-port mapping on every node; set it in the manifest.
- Run as UID/GID 65534. State/content directories require mode 0700 and ownership
  permitting that user to write.
- Replicate `/data` only. The operator configures `/var/lib/drop-cluster` and all
  private certificates/credentials as unsynchronized at deployment time.
- Keep `/tmp` writable; secrets must never be in project content or image layers.
- The supervisor runs app, Nginx and the provisioned coordinator together; an
  unexpected child exit stops the unit. Allow 25 seconds for shutdown.
- Docker health checks test app and coordinator liveness, not quorum authority.
  Application `/readyz` requires a fresh quorum-backed read.
- `DROP_MAINTENANCE_ENABLED=true` enables bounded metadata cleanup, including
  abandoned reservations and expired anonymous projects/sessions/grants. It does
  not delete replicated content or refund retained-byte charges.
- Content-peer fallback remains separately configured in [PEER_SETUP.md](PEER_SETUP.md).

Preserve Raft state across restarts. A node losing its durable log must receive a
new identity and join as a learner. Discovery alone never bootstraps or removes
voters. Automatic replacement requires a surviving quorum, a credential-approved
learner, successful catch-up, and an unhealthy-voter grace period.

## Build and verification

```sh
go test -race ./...
go vet ./...
docker build -t YOUR_REGISTRY/flux-drop:raft-test .
```

Go tests include real three-node Raft/mTLS metadata operations, authenticated
leader routing, ownership/session revocation across a partition, quorum-loss
denial, and private-credential enrollment/promotion/replacement.

The credential-free Docker lifecycle suite runs the actual app image on three
nodes without external networking or Firebase/Firestore emulators:

```sh
docker compose -f tests/raft/compose.yaml up --build -d
node tests/raft/run.mjs
docker compose -f tests/raft/compose.yaml down -v
```

See [the test boundaries](../tests/raft/README.md). This suite passed publishing,
cross-node sessions, private-password revocation, leader loss, quorum-loss denial
and restart recovery. Its shared content volume does not simulate Flux replication.

The existing Docker lifecycle suite still runs an explicitly isolated legacy
emulator backend:

```sh
docker compose -f tests/staging/compose.yaml up --build -d
node tests/staging/run.mjs
docker compose -f tests/staging/compose.yaml down -v
```

That backend requires `DROP_METADATA_BACKEND=firestore-emulator`, development
mode, a `demo-` project and both emulator endpoints. It cannot be selected for a
production Firebase project. This suite is a regression check, **not acceptance
of the Raft deployment or real Google OAuth**. Its disposable test volume is
removed by `down -v`.

## Outstanding release gates

Live Google OAuth, live Flux
port/persistence/content-replication checks, and
content durability acknowledgements across replica loss remain outstanding.

Existing Firestore metadata is not automatically imported. Use an explicitly
fresh test deployment or a reviewed migration; do not assume pointing Raft at old
content will recover ownership. Existing files are not deleted by this change.

Google sessions are bounded by verified Firebase token expiry (normally up to one
hour). Users reauthenticate after expiry. Public-key verification cannot detect
Firebase-side account disable/revocation immediately; Drop logout/session rotation
and password-policy changes are enforced through current quorum-backed metadata.

# Cluster coordinator: implementation and rollout boundaries

For current automatic deployments use [FLUX_DEPLOYMENT.md](FLUX_DEPLOYMENT.md).
It supersedes the manual CA/manifest requirements below. All node-local files,
including `cluster-node.json`, now reside under `/var/lib/drop-cluster`.

The coordinator is implemented as `cmd/drop-cluster` with the reusable Go package
`internal/cluster`. It runs discovery, authenticated status and Raft consensus.
**The source production runtime now uses this coordinator for metadata.** Firebase
is authentication-only and service-account credentials are rejected. Previously
published images are unchanged. Certificate provisioning and final deployment
acceptance remain unfinished; this is not yet an unrestricted production release.

## What is implemented

- Hashicorp Raft 1.7.3, pre-vote enabled, one leader, majority-committed changes.
- Fsync-enabled local Raft log/stable storage using an embedded bbolt file; no
  external database server. Two retained snapshots, bounded deterministic
  metadata records and atomic compare-and-set transactions.
- Private, node-bound persistent state; refusal to bootstrap a singleton, reuse
  another node/cluster's state, or silently rebootstrap an identity with lost history.
- Quorum verification and a committed barrier before authoritative reads/readiness.
  A role/status string is never authorization. Timeout may mean an indeterminate
  write outcome; future application operations must preserve idempotency keys.
- TLS 1.3 mutual authentication, application identity checks, expected destination
  node-ID pinning, cluster-specific Raft ALPN, bounded handshake concurrency.
- Periodic validated Flux discovery and bounded authenticated status polling.
  Discovery observations cannot change membership or elect a leader.
- Private-credential/mTLS learner admission, live catch-up checks before promotion,
  and serialized quorum-committed replacement with a three-voter minimum. A
  five-minute failure grace and direct probes precede replacement. Discovery
  omission alone is not evidence that a committed member is unhealthy.
- Credential-free Firebase verification and token-expiry-bounded sessions selected
  by the production entrypoint. Legacy Firestore is limited to emulator regression tests.
- Session/project/reservation/quota/grant adapters, durable owner indexes and
  authenticated one-hop leader routing. Read sets are checked at a quorum-fenced
  point; writes check every record used for authorization. Unknown commit outcomes
  are never automatically replayed.

Raft assumes trusted participants with crash/network failures; it does not defend
against a malicious voter or a compromised host/private key. A dedicated CA and
controlled enrollment are mandatory. Public discovery is not proof of identity.

## Storage and identity

`/data` remains Flux-replicated immutable project content. Provision a **different,
node-local persistent mount**, proposed `/var/lib/drop-cluster`, owned by UID 65534
and mode 0700. Do not synchronize that mount with Syncthing/Flux or clone it to
another live node. It contains `identity.json`, `raft.db`, and snapshots.

The mount must already exist. The coordinator refuses missing, public-readable,
symlinked or overlapping state/content roots. It cannot determine whether an
operator configured external synchronization on an otherwise valid mount; that
must be checked in the Flux deployment specification.

**Flux-specific caveat:** the local Flux implementation synchronizes the entire
component volume, including additional mounts. Merely adding
`m:raft:/var/lib/drop-cluster` to the same `r:/data` component does **not** establish
an independent unsynchronized volume. This was checked in
`ZelBack/src/services/appMonitoring/syncthingMonitor.js` and
`docs/multiple-mounts-guide.md` in `/root/flux` (the guide's sync section).
The operator will configure replication exclusions at deployment time. The app
uses one image/component and does not attempt to manage Flux volume settings.
Ensuring the Raft volume is excluded remains a deployment prerequisite.

The [Flux component documentation](https://docs.runonflux.com/fluxcloud/register-new-app/deploy-with-docker/components/)
also describes unprefixed volumes as local, unsynchronized and non-persistent.
Do not assume that data survives container replacement: confirm actual restart
semantics. A replacement without its old durable log must join as a **new node**,
not resume a copied identity or bootstrap another cluster. A surviving quorum is
still required; this does not solve simultaneous loss of all local state.

Use one stable certificate/node ID per instance, including across IP changes.
The approved managed mode distributes a dedicated cluster CA privately to trusted
replicas. Compromise of any replica compromises cluster identity. Never put the
CA or node keys in `/data`, image layers, logs, or public deployment specifications.
Manual certificate provisioning remains available via [PEER_SETUP.md](PEER_SETUP.md).
Generate one random 128-bit cluster ID once, and provision the same ID and trust
bundle to the intended members. A cluster ID is a namespace, not a secret or
substitute for authentication.

Automatic membership enrollment is optional, but it never chooses a new cluster
when the old membership is unavailable. Confirm node-local persistence and
private CA delivery before enabling deployment.

## Managed certificates (one component)

Create the CA once on a trusted machine using the image's `drop-cluster` binary:

```sh
drop-cluster -create-ca /private/cluster-ca.json -app YOUR_APP -cluster-id YOUR_32_HEX_CLUSTER_ID
```

The command creates a mode-0600 file and refuses to overwrite existing material.
Deliver this same private bundle to every trusted replica through protected
configuration. Set `caBundleFile` to its absolute path in the node manifest, or
supply its base64 encoding through **private** `DROP_CLUSTER_CA_BUNDLE_B64`, not both.
In managed mode omit `certificate`, `key`, and `ca`: they default to `node.crt`,
`node.key`, and `ca.crt` inside the unsynchronized state directory. Other explicit
TLS paths are rejected in managed mode. Existing manually provisioned clusters
require a reviewed migration, not simply enabling the new environment variable.

The supervisor provisions TLS before starting its children. Each node generates
its own Ed25519 key. Seven-day certificates renew when less than 48 hours remain;
the coordinator checks hourly and TLS clients/servers reload renewed leaves on
new handshakes without restarting Raft. Renewal preserves the node key and identity.
Missing keys on established nodes fail closed. Keep the state volume local and
durable; replacement nodes without it need new identities. The five-year root CA
requires planned operator rotation; changing the CA in place is deliberately rejected.

Managed mode derives a cluster-scoped admission credential from the signing key,
so a separate enrollment secret is unnecessary. An explicit admission credential
can still override this on all nodes. Initial three/five-voter bootstrap, node IDs,
addresses and the manifest remain explicit; CA possession never auto-bootstraps
a new voting configuration after quorum loss. This automation concerns coordinator
TLS, not the separately configured optional content-peer listener.

## Example node manifest

Save outside `/data`; use actual routable IPs and externally mapped Raft ports.
The addresses below are documentation-only placeholders:

```json
{
  "app": "YOUR_FLUX_APP",
  "clusterID": "REPLACE_WITH_32_LOWERCASE_HEX_CHARACTERS",
  "local": {"id": "node-one", "address": "192.0.2.10:18445"},
  "stateDir": "/var/lib/drop-cluster",
  "contentDir": "/data",
  "listen": "0.0.0.0:8445",
  "statusListen": "0.0.0.0:8446",
  "statusPort": 18446,
  "selfIPs": ["192.0.2.10"],
  "certificate": "/run/secrets/peer.crt",
  "key": "/run/secrets/peer.key",
  "ca": "/run/secrets/ca.crt",
  "initialVoters": [
    {"id": "node-one", "address": "192.0.2.10:18445"},
    {"id": "node-two", "address": "192.0.2.11:18445"},
    {"id": "node-three", "address": "192.0.2.12:18445"}
  ]
}
```

Exactly three or five initial voters are required for initial bootstrap. Their
manifest must be identical on all original members. Change only the local ID,
local address, self IPs and node-specific certificate paths per node.

**Supply `initialVoters` only for the planned initial cluster creation. Remove
it after successful bootstrap.** Replacement nodes start with `initialVoters: []`
and wait for explicitly approved learner admission. Existing durable membership
always takes precedence on restart. Empty discovery never triggers bootstrap.

```sh
go build -o bin/drop-cluster ./cmd/drop-cluster
bin/drop-cluster -config /run/secrets/cluster-node.json
```

This is a separate coordination process, not yet started by `drop-init`. The
Dockerfile includes the binary for controlled evaluation only; the default image
entrypoint remains unchanged. There is no Firebase credential requirement for
this coordinator itself.
The default supervisor starts the coordinator alongside Go/Nginx when its manifest
exists at `/run/secrets/cluster-node.json`. Production now requires that manifest;
absent or invalid provisioning fails app startup instead of falling back to Firestore.

## Endpoints

### Single image and Flux component (current decision)

Build the combined image with:

```sh
docker build -t flux-drop:test .
```

One image includes the app, Nginx and coordinator, supervised together as
UID/GID 65534. Ports are 8080 (public), 8445 (Raft) and 8446 (mTLS status).
The operator configures `/data` as replicated and `/var/lib/drop-cluster` as
unsynchronized at Flux deployment time. Both directories need owner 65534 and
mode 0700. Keep node certificates/keys and the manifest outside replicated storage.
The separate coordinator Docker target is removed; no second component is needed.

The Docker health check checks the app and, when provisioned, authenticates the
local coordinator status endpoint and verifies cluster and node identities.
Healthy followers pass; coordinator quorum loss alone does not cause a restart
loop. Application readiness must independently require quorum. An unexpected
exit from any supervised child stops the entire container.

The operator selected private cluster credentials for automatic enrollment.
Those must be supplied through private/Enterprise configuration or protected
secret files, never ordinary public Flux environment variables, image layers,
logs or synchronized content. Set `DROP_CLUSTER_ENROLLMENT_KEY` to base64-encoded
32 random bytes, or configure `enrollmentCredential` as an absolute private-file
path in the manifest (not both). The runtime still requires unique node TLS
certificates and an explicit initial voter manifest. Enrollment verifies and pins
the destination certificate before transmitting the credential, commits the
candidate identity/key before admission, and admits replacements as learners.
Retired identities cannot reenroll; a lost voter log requires a new identity.
Certificate issuance/renewal is automatic with the private CA bundle above. No shared Raft node keys
or automatic rebootstrap after quorum loss are provided.

### Authenticated status routes

All endpoints below are on the separate **mTLS status listener**, not public Nginx.
They require exactly one `X-Drop-Cluster` header matching the cluster ID. Requests
with browser cookies, Authorization headers, bodies, queries or methods other
than GET are refused for these observational routes. The internal POST metadata
route additionally requires a current Raft member certificate; the POST enrollment
route instead requires a valid candidate certificate, the private admission
credential and independent fresh discovery/status verification. Neither is public.

| Endpoint | Meaning |
| --- | --- |
| `/healthz` | Process/listener is alive; not evidence of authority |
| `/readyz` | This node is leader and has just verified quorum and a read barrier; followers/minorities return 503 |
| `/_drop_cluster/status` | Node ID, leader ID, role, uptime, term, queued/applied metadata indices; observational only |
| `/_drop_cluster/peers` | Fresh authenticated discovery candidates, not the voting configuration; stale discovery returns 503 |

`appliedIndex` is Raft's queued-to-FSM index; `stateIndex` is fully consumed
metadata state. Catch-up checks require both durable log progress and actual
state-machine progress. Uptime/IP hashes are not consulted for elections.

Flux locations are refreshed approximately every 45–75 seconds by the existing
discovery implementation. Status is sampled every 15 seconds with bounded
parallelism, TLS identity binding, cluster-ID checks and response limits. Reported
candidates expire; absent/unhealthy nodes remain in voting membership until a
separate approved membership change commits.

## Replacement and failure policy

1. Authenticate and explicitly approve a replacement's unique identity/address.
2. Add it as a nonvoting learner through the current leader.
3. Wait for authenticated evidence that committed metadata has been applied.
4. Promote it through Raft, then remove the retired voter through Raft.
5. Never reduce the voting configuration to only currently reachable nodes.

Three voters tolerate one failure; five tolerate two. Sequential churn is
manageable while the old quorum survives long enough to admit replacements.
Losing a majority permanently requires a reviewed disaster-recovery procedure;
this implementation deliberately has no automatic force-new-cluster switch.

Content durability is separate: metadata commit alone does not prove an upload
has reached another content replica. The publishing migration must define the
content-availability acknowledgement policy before promising failover durability.

## Verification and remaining integration

```sh
go test -race ./internal/cluster -count=5
go test -race ./internal/session
go vet ./...
```

Tests cover isolated-old-leader denial, majority failover, total quorum loss,
concurrent CAS, snapshot/restart recovery, real mTLS identity/cluster boundaries,
status access controls, discovery versus membership separation, learner catch-up,
promotion/removal, runtime startup/shutdown, and local-state safety checks.

Remaining release gates (service-account credentials are already removed from
the source production path):

- Reviewed import or explicit fresh start for existing Firestore metadata.
- In-place node address changes and planned root CA rotation; replacements
  currently need new identities and their own node manifests.
- Live Flux verification of node-local persistence and dedicated port mappings.
- Browser renewal UX (currently reauthenticate after expiry). The selected
  verifier rejects unsigned Auth-emulator mode and does not detect Firebase-side
  disable/revocation instantly. Authentication is bounded by token expiry.
- End-to-end publishing/private-access tests across leader loss and replacements,
  then a new verified Docker image. The currently published image is unchanged.

# Cluster coordinator: implementation and rollout boundaries

For current automatic deployments use [FLUX_DEPLOYMENT.md](FLUX_DEPLOYMENT.md).
It supersedes the manual CA/manifest requirements below. All node-local files,
including `cluster-node.json`, now reside under `/var/lib/drop-cluster/private`.

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
- Private, node-bound persistent state; refusal to recover a singleton implicitly, reuse
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
still required for automatic admission. Explicit operator-file recovery is described
in [Majority-loss recovery](#majority-loss-recovery).

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
unsynchronized at Flux deployment time. The automatic runtime creates an
owner-only `/var/lib/drop-cluster/private` child for certificates, keys and the
manifest; Flux owns the bind root and may widen its permissions. See
[FLUX_DEPLOYMENT.md](FLUX_DEPLOYMENT.md) for the live Flux permission caveat.
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

### Explicit eventual serving reads

Site delivery, its post-fetch checks, thumbnails, private serving session/grant
reads and Explore use the local coordinator's coherent applied snapshot. There
is deliberately no replication-age limit: disconnected followers may serve old
public policy/digests or keep denying newly allowed access indefinitely. Known
expiry timestamps are still checked against the current clock. Management reads,
unlock creation, all security changes and writes retain their leader/CAS paths.
Leaders retain async-view authority requirements; unusable local coordinators
return 503 with Retry-After rather than falling back globally.

Only absent required records trigger a complete logical leader lookup, bounded
per primary to 64 active distinct confirmations and 256 waiting callers. Equivalent
lookups share records; authorization callbacks run separately per caller. Confirmed
missing public slugs have a bounded 1024-entry, one-second negative LRU. Exhaustion
fails closed with 404/denial; transport failures are never negatively cached.
Aggregate maximum confirmation concurrency is 64 times the number of primaries.
Public project resolution takes two loopback snapshot RPCs (slug then project),
with no final Check RPC. Preview IO retains final snapshot validation. When a
preview requires missing-record leader confirmation, each caller also validates
its replayed IO against that leader; ordinary local preview IO stays local. Private
access additionally reads session and a coherent session/grant/project snapshot.

The authenticated local_snapshot RPC always targets the local coordinator, follows
no leader hints and cannot update cached leader routing. Stamps include process
identity, view kind, term, applied index and restore generation. Dependent reads
retry the whole lookup on conflict. Upgrade coordinators before HTTP processes:
older coordinators reject this RPC, producing clear serving unavailability; there
is no silent global fallback. Public browser and Explore caches have independent
TTL windows, in addition to unbounded local metadata replication delays.

## Majority-loss recovery

Flux specification updates can replace instances and permanently remove their
node-local `ml:state` volumes. Replication of `/data` does **not** replicate the
Raft log or identity. With three voters, permanent loss of two stops elections;
empty replacements cannot admit themselves. A healthy Docker container can
remain in that state indefinitely. Alert externally on application `/readyz`
(failed authority/metadata service), not Docker health status. Confirm that all
three voters are healthy and that a recent encrypted backup exists **before**
any update; replace one voter at a time and wait for three healthy voters again.

Recovery is destructive consensus surgery, never an availability timeout. First
confirm the other voters are permanently gone and fence them against returning.
Exactly **one** node may perform either recovery procedure in a cluster. Two
independent recoveries, even from the same backup, can create conflicting
histories and must never run. Do not copy identities, keys, request files or Raft
volumes onto additional live nodes. Keep the original cluster identity, app and
private passphrase. Fresh replacements use their own new identities and the
normal authenticated learner-admission path.

Only an operator-written request file in the node's configured `StateDir` can
request recovery. In automatic deployments that directory is
`/var/lib/drop-cluster/private`, not the mount root. There is no recovery network
API, environment switch, command flag, discovery decision or automatic timeout.
`drop-cluster -status` and `-healthcheck` only query the authenticated running
listener; they never open or recover the database. Writing a request while the
coordinator runs has no effect until its next normal startup.

### Choose a recovery source

Prefer the surviving voter with the most complete trustworthy log. Its committed
metadata, sessions, ownership indexes and project records are retained.
**Hashicorp RecoverCluster implicitly commits its entire available log tail**,
including entries that may not previously have committed. Records existing only
on lost voters, or only in a speculative async journal, cannot be recovered.
Keep the complete off-node original so this decision can be audited.

If no node has usable state, restore an authenticated encrypted metadata backup
onto one empty node. All metadata writes after that backup are lost. That can
restore old session validity, ownership, privacy or deletion state, as well as
lose newly published projects. Files in `/data` cannot reconstruct those missing
security records. Treat the rollback as a security incident and review affected
access and revocations. Recovery does not alter serving or authorization policy.

### Surviving voter: one-shot singleton recovery

1. Fence all permanently lost voters. Record the selected survivor's authenticated
   status with `drop-cluster -status`: cluster ID, node ID, term and `lastIndex`.
   Verify the local manifest and identity agree. Do not use a discovery index or
   an FSM revision as the expected log index. A snapshot-only store can have
   `lastIndex: 0`; the exact value must still be supplied explicitly.
2. Stop the coordinator and make a protected **off-node backup of the entire
   StateDir**, including `raft.db`, `snapshots/`, identity, journal, manifest and
   private keys, plus surviving encrypted metadata backups and project content.
   A raw Bolt copy must be made while the coordinator is stopped. Retain the
   passphrase separately in a secure vault. Never copy these private files into
   `/data` or a public archive.
3. Write this request on the selected node as the container's user (UID/GID
   65534 in the image). With the container stopped, use its host volume path
   or a maintenance container mounted to that volume, without starting another
   coordinator. Substitute reviewed IDs and the exact recorded index;
   the example index is not a default:

   ```sh
   STATE=/var/lib/drop-cluster/private
   umask 077
   cat > "$STATE/recover-singleton.json" <<'JSON'
   {"clusterID":"REPLACE_WITH_CLUSTER_ID","nodeID":"REPLACE_WITH_SURVIVOR_ID","expectedLastIndex":135064}
   JSON
   sync
   ```

   The file must be a regular file owned by Drop's effective UID. Symlinks,
   unknown/duplicate fields, malformed JSON, identity mismatches, an index
   mismatch, missing history, and a node that is not a voter in the latest
   persisted configuration fail startup. Refusal preflight opens Bolt read-only;
   it does not rewrite `raft.db`.
4. Perform a **soft restart/redeploy on that same host and existing local
   volume**. Verify that the chosen Flux action preserves `ml:state`; do not
   make a specification update or hard redeploy that destroys the survivor.
   Before mutation, startup copies and fsyncs `raft.db`, `snapshots/` and
   `identity.json` under `pre-recovery-<UTC timestamp>/`. A copy failure aborts.
   RecoverCluster uses a scratch FSM; the running Raft instance gets a fresh FSM.
5. Check the warning log, restored metadata and authenticated status. Success
   renames the request to `recover-singleton.done-<timestamp>` and fsyncs the
   directory. The recovered node must elect itself with one voter. Keep the
   automatic pre-recovery copy and the independent off-node archive.
6. Start two fresh replacement instances using normal private credentials and
   discovery. Verify learner admission, catch-up and promotion back to **three
   voters**. Replicated user/security writes remain blocked until a second voter
   joins in async mode; cluster-internal enrollment writes can proceed. Verify
   representative sessions, owner project listings, indexes, new writes and
   readiness before reopening management traffic. Do not declare a singleton a
   repaired production cluster. Verify that stopping any one rebuilt voter still
   leaves a functioning majority.

The consumed request prevents recovery repeating on normal restarts. Keep the
`.done-*` files for audit. If a recovery or request-consumption step fails, startup
fails; do not edit the expected index until the changed disk state and original
backup have been reviewed. Interrupted recovery may have partially installed a
snapshot; restoring the off-node original before retry is an operator action.
Never remove a `.done-*` suffix to rerun recovery against a running cluster.

### Encrypted metadata backups

The leader checks every **15 minutes** and writes only when its committed FSM
history changed. After a restart or leadership change, the last available backup
is authenticated before using its history as an unchanged-state hint. The speculative async view
is excluded. Files live only in `/data/metadata-backups/`, outside every Nginx
alias and project-content path. They contain the full metadata FSM, not project
file bytes or node TLS keys.

AES-256-GCM encrypts the snapshot in memory before any replicated-volume write.
HKDF-SHA256 derives its key from the existing `DROP_CLUSTER_PASSPHRASE`, with the
cluster ID as salt and dedicated info `flux-drop/metadata-backups/v1`. Cluster ID,
index, leader term at capture and creation time are authenticated. A fresh random
nonce is used for every file. Retain the original strong passphrase independently
of the Flux hosts; a wrong key or altered file fails authentication. Explicit
manual-credential deployments without this secret cannot produce or restore
these encrypted backups: failures are reported without affecting metadata service.
No new environment variables or recovery switches are added.

Files are atomically renamed only after fsync; the directory is fsynced. The six
newest encrypted files for this cluster are kept. The plaintext snapshot is capped
at 256 MiB (encrypted file cap: 256 MiB + 8192 bytes), and a write requires 2 GiB
free headroom plus twice the candidate encrypted-file size, preserving the default
upload-admission margin. Provision space for
six snapshots **in addition to project content**. Backup and retention failures
increment `backupFailures` and log at most once per five minutes. They never
fail writes, serving or readiness. Export that counter and alert on missing/stale
backups independently. Copy backups off-node too: Syncthing replication is
asynchronous, and a successful local write does not prove a second durable copy.

### No surviving state: one-shot backup restore

1. Confirm every old voter is permanently gone and fence it. Choose **one** new
   node and provision its normal identity/TLS/manifest with the original app,
   cluster ID and passphrase. Stop its coordinator. It must have no existing
   Raft log entries, stable term or snapshots. A matching pre-provisioned node
   identity is permitted; usable or incomplete Raft history is refused.
2. Back up surviving `/data` and the selected encrypted backup **off-node** before
   any write. Choose an exact `.enc` file from the surviving
   `/data/metadata-backups/` directory. There is no automatic "latest backup"
   choice. Confirm its age and explicitly accept the loss of all later metadata.
3. As the container UID, write the following regular file, then `sync`:

   ```json
   {
     "clusterID": "REPLACE_WITH_ORIGINAL_CLUSTER_ID",
     "nodeID": "REPLACE_WITH_NEW_NODE_ID",
     "backupFile": "/data/metadata-backups/REPLACE_WITH_EXACT_BACKUP_FILENAME.enc"
   }
   ```

   Save it at `/var/lib/drop-cluster/private/restore-singleton.json`. The backup
   path must be an exact, clean absolute regular filename. An off-node copy
   staged in the node-local `recovery-backup/` directory is also accepted. File
   links, parent-directory links and oversized files are refused. Do not also supply `recover-singleton.json`.
4. Soft restart the selected node, preserving its new local volume. The backup
   is fully authenticated, decrypted and FSM-validated **before** opening a
   writable Raft store. Startup installs its state as a sole-voter snapshot,
   preserves the revision ceiling and term, and atomically consumes the request
   as `restore-singleton.done-<timestamp>`. A warning identifies the backup date
   and explicitly reports loss of subsequent writes.
5. Follow the same two-replacement enrollment, three-voter verification,
   metadata/security review and readiness checks as survivor recovery. A second
   normal restart must not restore again. Never restore the same backup on a
   second node; enroll it empty instead.

A failure leaves startup stopped for operator review. If installation was
interrupted after verification, local history may already exist: retry refuses
it rather than overwrite it automatically. Retain the original encrypted backup
and prepare another reviewed empty node only after fencing the failed attempt.

### Operational status and alerts

Authenticated `/_drop_cluster/status` and `drop-cluster -status` add:

- `voters` and `learners`: counts in the latest local Raft configuration.
- `lastContactAgeSeconds`: follower age since leader contact; `null` if none
  recorded, and zero on a leader. This is observational, not authority proof.
- `lastSuccessfulBackup`: UTC time of the last successful encrypted
  backup written or validated by this process, or `null` before one is known. It is not proof of off-node replication.
- `backupFailures`: this process's cumulative backup/retention failure counter.

A separate observational worker emits a rate-limited `slog.Error` after more
than two minutes without a leader, including voter IDs unreachable by direct,
identity-checked mTLS status probes. An empty node waiting over five minutes for
admission logs a clear warning. Neither timer changes membership or triggers
recovery. Docker health semantics are unchanged; external readiness and backup
age alerting are still required.

### Dynamic membership and returning spares

The stored membership policy remains `{ "voters": 3 }` or `{ "voters": 5 }`;
its value is the minimum voter count. Healthy, committed, directly authenticated
members can raise the target to 5 or 7 after five continuous minutes. Lowering
the target takes fifteen minutes and removes only failed voters after the
replacement grace. A healthy spare replaces a failed voter without waiting for
the target windows. At most sixteen members and seven steady-state voters are
supported (an eighth voter is permitted briefly for replacement).

Health retirement preserves an admitted identity's key pin. A returning node
with the same key can rejoin as a learner after losing leader contact for two
minutes; it regains its vote through normal catch-up and target rules. Existing
bootstrap voters have their verified keys pinned while healthy. An already
retired legacy identity without a recorded key cannot be safely re-pinned.

Hashicorp Raft steps down on a higher-term replication response even from a
learner. Such a returning node is therefore deferred until its term is no
higher than the leader's, preserving its data and current leader authority.
Pre-vote alone does not solve this case. Removal also cannot cancel an RPC
already in flight; an immediate high-term restart can still complete that RPC.

### Recovery generations and superseded histories

Both singleton recovery paths now write `cluster_recovery/generation` after
first becoming leader, before admitting returning histories. An absent record
means generation zero. An absent, unretired generation-zero history cannot be
certified by that handshake: it may predate a legacy recovery. It keeps waiting
for operator review/reset instead of rejoining with unverifiable applied state.
Same-key health retirees still use normal equal-generation rejoin.

A private, fsynced intent makes that commit restartable;
its random lineage distinguishes independently recovered histories. A generation
high-water mark on the replicated content volume also prevents a known older
backup from reusing a generation. If all evidence of a newer history is lost,
no local counter can prove that it once existed: keep off-node backups and never
run independent recoveries.

Returning nodes use the credential-protected mTLS `/_drop_cluster/rejoin`
protocol without changing the legacy enrollment body. An older peer returning
404 means wait. Equal-generation health retirees keep their state; older
histories receive `reset_required`. A node may act on that advice only without
its own leader and without a reachable majority of its old generation. Runtime
then closes Raft, journals an atomic move of `raft.db` and `snapshots/` into
`superseded-<UTC timestamp>/`, and requests a container restart. Identity and
keys remain local. Only the newest superseded archive is retained. Interrupted
moves resume before opening Raft; a reset node enrolls empty and catches up.
The singleton user/security-write block is unchanged.

### Recovery view and a learner as last surviving source

`GET /_drop_cluster/recovery-view` is available only on the existing mTLS
listener, with a valid application identity and `X-Drop-Cluster`. It reports
`nodeID`, `clusterID`, `lastLogIndex`, `lastLogTerm`, `hasLeader`,
`membershipRole` (`voter`, `learner`, `none`), `discoveryViewHash`,
`evaluationState`, `recoveryMode`, `recovery` (`generation`, `lineage`) and
`protocol`. Peer decoding tolerates extensions but validates known fields and
binds the node ID to its certificate. A 404 is never agreement. None of these
fields was added to the rolling-upgrade-sensitive `Status` JSON contract.

If **no voter survives**, a fully caught-up learner may be the manual recovery
source. Confirm that independently before proceeding: the request cannot prove
that another host is permanently gone. Add `"allowLearner": true` to the usual
`recover-singleton.json` request, retaining the exact `expectedLastIndex`.
Without that explicit field, learner recovery remains refused without changing
Raft state. All normal backups, identity checks, request consumption and the
single-recovery-only rule still apply. A learner can lack acknowledged writes
that reached voters; review the resulting metadata and security state.

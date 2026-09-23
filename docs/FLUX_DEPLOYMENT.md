# Passphrase-only Flux deployment

This applies to the new source/local image, not previously published Docker Hub
tags. One component runs Nginx, the application and the coordinator.

## Configuration

Supply one **private** setting, identical on every replica:

```text
DROP_CLUSTER_PASSPHRASE=<at-least-32-bytes-of-strong-random-secret>
```

Use a password manager/random generator, not a guessable phrase. Peer certificates
permit offline guesses against weak secrets; Argon2id raises their cost but cannot
make a weak secret strong. Never put this secret in a public Flux specification,
logs, project files or image layers. All holders are trusted cluster administrators.
No Firebase private credentials are used.

Flux supplies `FLUX_APP_NAME` and `FLUX_NODE_HOST_IP`; the local
`fluxnode.service:16101/hostinfo` endpoint is the fallback, retried on startup.
There is no guessed app/IP or failed-discovery-as-singleton shortcut.

Configure these port mappings in Flux (not extra environment variables):

| Container port | External mapping | Purpose |
|---|---|---|
| 8080 | Public HTTPS ingress | Nginx/UI/static sites |
| 34444 | 34444 on every node | Authenticated one-hop content fallback |
| 34445 | 34445 on every node | Coordination/replication |
| 34446 | 34446 on every node | Authenticated status/enrollment/metadata |

The dedicated ports must be available and identical across nodes. The image does
not infer arbitrary encrypted-spec port mappings. Never expose loopback port 8081
or route private cluster endpoints through public Nginx.

| Persistent volume | Replication |
|---|---|
| `/data` — immutable project content and deployment-existence marker | Enabled |
| `/var/lib/drop-cluster` — ALL node-local configuration, identity, keys, journal, Raft log/snapshots, observations and upload staging | Disabled |

The operator configures Flux replication exclusions. Both roots need mode 0700 and
ownership UID/GID 65534; fresh Docker volumes inherit the image's directory ownership.
Size the node-local volume for uploads as well as Raft state: the default upload
limits reserve roughly 700 MiB per active upload plus 1 GiB free-space headroom.
Keep `/tmp` writable and allow up to 200 MiB per concurrent peer fallback for
temporary, verified response spooling. Upload staging is node-local; a brief
`.install-*` copy inside `/data` is verified before atomic publication and
removed after failure. Never copy a node-local volume to another running replica.
Allow 25 seconds for shutdown. No secret/config volume or per-instance commands
are required.

## Startup and recovery

The supervisor derives an app-scoped private CA from the passphrase, generates a
random persistent node ID and unique leaf key, writes its manifest, and provisions
TLS before starting its children. All generated private files stay in the local
volume. Leaves renew automatically; both coordinator and content-peer handshakes
reload them. The deterministic v1 root expires in January 2045. Changing the
passphrase/app on existing storage fails closed; rotation requires a planned migration.

Initial discovery must include this node and remain stable for 30 seconds. With
one listed node it may initialize coordination. With multiple nodes, the lowest
IP-hash candidate requires every listed peer to be reachable, authenticated,
uninitialized and reporting the identical discovery view. Later nodes enroll as
learners, catch up, and are promoted toward three voters. Deploy at least three
replicas. Security/session writes wait for another voter during singleton startup.

**Bootstrap assumption:** initial Flux discovery is trusted to describe the whole
fresh deployment. Matching views cannot prove the absence of an unlisted old
cluster. The replicated genesis marker blocks initialization when earlier
deployment evidence exists, but asynchronous marker propagation is not an atomic
external bootstrap registry. Initialization is not guaranteed safe against arbitrary
incomplete discovery or simultaneous total loss of all durable state.

After initialization, persisted membership—not API-list changes—controls write
authority. Raft remains for elections, membership and security barriers; content
requests no longer await replication. Heartbeats renew a short authority window.
An isolated old primary stops serving authoritative metadata; a majority elects
a log-compatible replacement. This may happen before an IP disappears from Flux.
Unhealthy-voter replacement has a two-minute grace, requires a surviving quorum
and a caught-up replacement, and never shrinks an established cluster to one voter.
A returning node with its original key/log and a new IP can have its address updated
by the quorum. An empty replacement gets a new identity. Majority loss requires
restoring enough original state or reviewed recovery; no timeout overrides security
history and no automatic destructive rebootstrap is performed.

Content is fsynced locally before acknowledgement but may be lost after primary
failure. Security changes wait for the ordered replication stream, including earlier
dependent metadata. A 64-entry queue applies backpressure. Unknown security outcomes
fence the writer and request leadership transfer, rather than serving an older policy.
See [DURABILITY.md](DURABILITY.md).

Authenticated status includes term, local durable revision, replicated revision and
committed history digest. Local primary history retains observed high-water marks;
promotion behind known content history is logged. Uncommunicated local writes
cannot always be detected after loss.

## Defaults and acceptance boundary

Publishing, metadata cleanup and authenticated content fallback are enabled.
The UI is public; the cluster passphrase is NEVER used for browser authentication.
Optionally set a separate `DROP_STAGING_PASSWORD` to restrict a test deployment.
Origin and public Firebase settings default as before. Google login still requires
the app hostname authorized in Firebase. Metadata cleanup does not delete replicated
content files or reclaim all retained-byte charges.

The Docker health check is liveness; `/readyz` checks current metadata authority.
Live Flux persistence/port mapping, actual content replication and Google OAuth
remain deployment acceptance tests. The isolated Docker lifecycle uses shared
content to test metadata failover, not to simulate Syncthing durability. Old
Firestore/manual-cluster data is not automatically migrated; use fresh test volumes
or reviewed migration, and do not mix old and new protocol images.

# Replica discovery

## Multi-instance lifecycle verification

`TestMultiInstanceRuntimeLifecycle` now starts two actual runtime listeners with
separate temporary data roots, certificate/key files, shared test CA and unique
instance identities. Private test hooks map public-looking discovery addresses to
loopback sockets and substitute a bounded discovery response; production has no
environment switch to disable IP/TLS checks. No Flux or Firebase calls are made.

The test sends requests through the real public delivery handler into fallback
and the remote mTLS listener. It covers no installed version, remote installation,
partially replicated content, recovery, no local disk cache, remote private-policy
revocation despite stale public ingress metadata, policy-revision mismatch, peer
shutdown, worker termination and listener closure. It passed five consecutive
race-enabled runs. Metadata uses an in-memory resolver; real Flux networking,
certificate deployment and Firestore-backed multi-instance staging remain gates.

## Runtime configuration

The entrypoint supports opt-in `DROP_PEERS_ENABLED=true`, requiring Firebase and
all peer settings in `.env.example`: app name, unique instance ID, dedicated port,
public self IPs, existing data directory and certificate/key/CA files. Paths must
be absolute and credentials (including symlink targets) must live outside the
replicated data tree. Startup verifies the certificate chain and both EKUs.

The TLS listener binds all interfaces on `DROP_PEER_LISTEN_PORT`, defaulting to
`REPLICA_PORT` (1024–65535, excluding 8080/8081). Discovery dials `REPLICA_PORT`;
external/container mappings may therefore differ. Do not route it through public
Nginx. Self IPs come from explicit configuration or `FLUX_NODE_HOST_IP` when
available. See PEER_SETUP.md for identity issuance and per-node provisioning.

Startup launches discovery and local-only mTLS delivery and injects the fallback
dependency. Publishing requires separate explicit staging configuration as
documented in STAGING.md. Listener failures terminate the process. Shutdown cancels discovery
and drains/closes the listener before Firestore closes. HTTP deadlines and header
bounds are set. Certificates load at startup; rotation requires a restart.

Configuration and Go regression tests pass. Multi-instance runtime lifecycle and
staging tests, certificate provisioning/revocation/rotation remain release gates.
No real credentials or cloud resources were provisioned. Historical references to
pending runtime wiring below are superseded by this section.

`internal/replica.NewDiscovery(app, port, selfIPs)` constructs a discovery client.
`Run(ctx)` refreshes immediately, then every 45–75 seconds; cancellation terminates
the worker. `Snapshot()` returns a copied, sorted peer list and a freshness flag.
The runtime will supply `FLUX_APP_NAME`, `REPLICA_PORT`, and explicit self IPs when
the authenticated peer listener/fallback is connected. Discovery is not started
by the product entrypoint yet, and does not make requests to discovered peers.

Only the fixed HTTPS endpoint `https://api.runonflux.io/apps/location/<app>` is used.
Application names are bounded alphanumeric strings. Requests have ten-second
deadlines, no redirects or environment proxy, a 16 KiB response-header limit, a
1 MiB body limit and at most 1,000 records. Unknown record metadata is ignored;
`status`, array shape, matching application name, IP and expiry must be valid.
An invalid record rejects the complete refresh rather than partially installing
a potentially unsafe response. An empty successful array clears the snapshot.

IPv4, IPv6, IPv4-with-port and bracketed IPv6-with-port are accepted. Advertised
ports are discarded; the configured peer port is always used. IPv4-mapped IPv6
is normalized. DNS names, scoped addresses, private/link-local/loopback addresses,
shared address space, documentation ranges and conservative IPv6 transition ranges
are rejected. Only public unicast destinations are eligible. Self addresses are
excluded and addresses deduplicated, retaining the earliest advertised expiry.

Failed refreshes preserve the last validated list for at most five minutes.
Individual expiry is also checked on every snapshot. A stale/uninitialized list
must not be interpreted as authoritative absence by the future fallback handler.
Discovery is not authentication: peers still need authenticated TLS and one-hop
request validation before any project content or authorization can be forwarded.

## Fixture provenance and verification

`internal/replica/testdata/explorer.json` contains two unchanged records excerpted
from the public [explorer location response](https://api.runonflux.io/apps/location/explorer)
observed on 2026-09-22. Tests use a fixed clock; they do not call Flux or these IPs.
Additional synthetic cases cover IPv6, invalid/private addresses, duplicate expiry,
self exclusion, schema errors, oversized bodies, HTTP errors, snapshot isolation,
staleness and worker cancellation.

Run `go test -race ./internal/replica`.

## Authenticated peer transport

`PeerTLS` builds TLS 1.3 client/server configurations with mutual certificate
verification. Provision a dedicated application CA, and a distinct key and
certificate per instance. Each leaf requires both serverAuth/clientAuth EKUs,
DNS SAN `<lowercase-app>.peer.flux-drop`, and exactly one URI SAN
`spiffe://flux-drop/<case-sensitive-app>/<instance>`. Standard chain, validity,
usage and server hostname checks remain enabled; the additional identity check
rejects certificates for another application even under the same CA. A shared
DNS SAN enables verified TLS when dialing discovered IP addresses.

Never replicate private keys with project files. Certificate issuance, secret
mounts, rotation/revocation and the separate peer listener still need runtime
integration and staging validation; no certificate provisioning is performed by
this code. The CA must be dedicated to this service, not a general system CA.

`NewPeerClient` applies connect/handshake/header/overall timeouts, bounded
connections and response headers, no redirects, no environment proxy and no
automatic compression. It is not a general proxy: the future fallback selector
must restrict destinations to fresh discovery snapshots and enforce response
size/status/header rules. No fallback requests are issued yet.

`PeerBoundary` requires an already verified client certificate, one
`X-Drop-Peer-Hop: 1` header, GET/HEAD, no body, no cookies or Authorization, and a
canonical `/_drop_peer/content/<slug>/<file>` path without query parameters or
hidden/traversal segments. It must wrap a local-only content handler on the TLS
listener, never the public router or a recursive fallback handler. It does not
replace authoritative project/visibility checks. Tests perform real local mutual
TLS handshakes and reject missing/wrong-application client certificates, an
untrusted server CA, plaintext calls, invalid hops, credentials and unsafe paths.

## Local peer file delivery

`LocalDelivery(app, repository, dataRoot)` includes the peer boundary and has no
discovery/fallback dependency. The separate Go mTLS listener serves replica-to-
replica bytes; public local delivery continues through Nginx. Callers supply
`X-Drop-Content-Digest` and `X-Drop-Policy-Revision` in addition to the one-hop
header. Each request resolves authoritative metadata, denies private/non-live
projects and rejects stale digest/policy requests with 409 before disk access.

The handler verifies the complete local version and serves only manifest-listed
files, then verifies the exact open file descriptor before transfer. Published
version trees must remain immutable; this is not a sandbox against a privileged
local process changing files during a transfer. Concurrency is capped at eight
requests. The future listener must also set HTTP read/write/header deadlines.

Verified file responses carry content digest, policy revision and a file-hash ETag.
HEAD, ranges and conditional responses are processed only after authorization.
Missing manifest entries return 404; incomplete/corrupt/unavailable local versions
return 503, not an assertion of global absence. The handler writes no content cache.

Tests use a real mutual-TLS connection and installed content, covering GET/HEAD,
206/304, stale digest/policy, hidden metadata, revoked access and corruption.
Runtime listener wiring and the discovery-selected streaming fallback are still
outstanding; no public request is proxied to a replica yet.

## Discovery-selected fallback

`NewFallback` creates a verified peer client and limits concurrent fallback
requests to eight. `ProjectDeliveryWithFallback` calls it only after public/live
metadata authorization, when local version verification fails. The product
entrypoint does not inject it yet: runtime certificate/discovery/listener wiring
is still outstanding.

Each fallback request rotates its starting peer, attempts at most three fresh
discovery destinations, revalidates public IP/port, and has a 30-second total
deadline. It sends only the one-hop marker, active digest and policy revision.
Cookies, authorization, query strings, ranges and browser validators are not
forwarded. Private projects are never eligible.

Only a 200 response bound to the expected digest/policy, without content encoding
and with a known length no greater than 200 MiB, can be streamed. Public security
headers and content type are generated locally; peer cookies, internal redirects
and arbitrary headers are never copied. The receiver trusts the authenticated
peer's file verification; it does not buffer an entire file to rehash it before
delivery. Content is streamed without a disk cache. Mid-stream failures abort the
response instead of retrying after bytes have been sent.

Fallback currently returns full 200 responses (or HEAD), ignoring optional ranges
and cache validators. If-Match/If-Unmodified-Since fail conservatively with 412;
Nginx and peer validator unification is a follow-up compatibility task. A 404 is
returned only when every discovered peer was attempted and reported absence.
Stale/empty discovery, unavailable peers, invalid responses or untried peers after
the attempt cap produce 503. Local corrupt/missing content alone never proves
global absence.

Real mTLS integration tests cover remote bytes/HEAD with browser credentials
present at ingress but absent at the peer boundary. Response-selection tests
cover absence, outages, stale discovery, redirects and wrong version binding.

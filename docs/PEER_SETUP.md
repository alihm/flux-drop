# Peer identities for a Flux staging deployment

Each replica requires its own certificate/key and unique `DROP_INSTANCE_ID`, plus
the same dedicated CA certificate. Private keys belong outside `/data`. Never
ship the CA private key to replicas, put it in an image, or commit generated keys.
The operator must arrange per-node secret delivery; this utility does not enroll
nodes, run a public certificate authority, or alter Flux applications.

## Offline issuance

Create a new CA directory on a trusted operator workstation, outside runtime secrets:

```sh
go run ./cmd/drop-peer-cert -mode ca -out /secure/offline/drop-ca
go run ./cmd/drop-peer-cert -mode issue -ca /secure/offline/drop-ca \
  -app APPNAME -instance node-one -out /secure/runtime/node-one
```

Parent directories must exist. Output directories must not exist; the utility
never overwrites keys. Issue a separate directory for each replica. Each leaf
directory contains `peer.crt`, `peer.key` and `ca.crt`, but not `ca.key`. Deliver
only that replica's leaf directory as its runtime secrets. Ensure UID 65534 can
traverse the runtime directory and read the files, keeping the key private.

Certificates use Ed25519, both client/server EKUs, DNS SAN
`<lowercase-app>.peer.flux-drop` and URI
`spiffe://flux-drop/<app>/<instance>`. CA lifetime is one year; leaf lifetime is
seven days (clamped to CA expiry). Renew and restart before expiry. The runtime
loads certificates at startup; no hot reload or online revocation service exists.
For a compromised identity, stop/remove it and rotate trust as part of incident
handling; do not treat discovery removal alone as certificate revocation.

## Per-replica configuration

```text
DROP_PEERS_ENABLED=true
FLUX_APP_NAME=APPNAME
DROP_INSTANCE_ID=node-one
REPLICA_PORT=<externally-exposed-peer-port>
DROP_PEER_LISTEN_PORT=8444
DROP_REPLICA_SELF_IPS=<this-node-public-IP>
DROP_DATA_DIR=/data
DROP_PEER_CERT_FILE=/run/secrets/peer.crt
DROP_PEER_KEY_FILE=/run/secrets/peer.key
DROP_PEER_CA_FILE=/run/secrets/ca.crt
```

Map the chosen external peer port to container **8444** on every node. The runtime
dials discovered IPs at `REPLICA_PORT`, ignoring ports embedded in discovery
records. `DROP_PEER_LISTEN_PORT` defaults to `REPLICA_PORT` when omitted. Do not
route the peer listener through public Nginx or expose Go's management port 8081.
TLS 1.3 mutual authentication is mandatory; there is no insecure fallback switch.

If `DROP_REPLICA_SELF_IPS` is absent, the runtime uses `FLUX_NODE_HOST_IP` when
injected by Flux. This was checked against the local Flux source; confirm that
the deployed node release supplies it. Otherwise provision public self IPs
explicitly. The app name must match the discovery API registration and certificate
exactly. Self IPs must not contain other replicas' addresses.

Flux's discovery list does not establish certificate identities or provide private
keys. Before enabling multi-replica fallback, resolve how your chosen deployment
mode delivers a different leaf identity to each replica. Do not copy one node's
certificate or all nodes' private keys into a shared replicated directory. Until
per-node provisioning is available, use local-only smoke testing and do not claim
multi-replica availability.

## Checks

```sh
go test -race ./internal/replica -run TestMultiInstanceRuntimeLifecycle -count=5
```

This exercises real local mTLS listeners, one-hop streaming, missing/partial
content and policy changes. In Flux staging additionally verify discovery freshness,
external/container port mapping, secret permissions, self exclusion, expiry,
node restart, replica lag and private-content denial. Readiness only establishes
local serving prerequisites; it does not attest to the whole replica set.

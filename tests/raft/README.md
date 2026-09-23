# Credential-free Docker/Raft lifecycle

Run on a Linux Docker host with the test bridge subnet 172.29.188.0/24 available:

```sh
docker compose -f tests/raft/compose.yaml up --build -d
node tests/raft/run.mjs
docker compose -f tests/raft/compose.yaml down -v
```

The three app containers each run the actual image's app, Nginx and coordinator
with asynchronous content acknowledgements enabled. Each has only a content
volume and a node-local volume; generated fixtures now live inside the latter.
They have no Firebase admin credentials, no Firestore/Auth emulators, no public
port bindings and no external network. The host test runner connects directly to
their fixed internal bridge addresses. The fixture container creates disposable
private shared-CA bundles; app startup generates each unique node-local TLS key
and certificate. No leaf keys are supplied by the fixtures. Fixture files are created
once, never overwritten. Clean this test stack before a new `up` run.

Tests cover anonymous upload, cross-node session/ownership recovery, duplicate
detection, update/rename, private password access and revocation, stopping the
actual leader, total quorum-loss denial, recovery, full restart, delete and logout.
Google OAuth is not mocked or bypassed in the production image. Live Google login
and actual Flux deployment remain separate acceptance checks.

The content volume is shared solely to isolate metadata failover testing. This
does not test asynchronous Flux content replication, new-upload durability or
missing-content peer fallback. Each node has a separate unsynchronized Raft volume.

`down -v` removes only this stack's disposable fixture, content and state volumes.
The local image is `flux-drop:raft-test`; the test does not push registry tags.

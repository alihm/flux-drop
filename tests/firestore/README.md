# Firestore transaction tests

These tests use the official Firestore emulator on localhost, with a new random
`demo-` project ID for each test. They never load production credentials. Records
exist only in the emulator's memory and are discarded when its container stops.

From the repository root:

```sh
docker compose -f tests/firestore/compose.yaml up -d
DROP_TEST_FIRESTORE=1 FIRESTORE_EMULATOR_HOST=127.0.0.1:18080 go test -race -v ./internal/session ./internal/project ./internal/httpserver -run TestFirestore
docker compose -f tests/firestore/compose.yaml down
```

Wait for the emulator's "Dev App Server is now running" log before the test run.
Without `DROP_TEST_FIRESTORE=1`, integration tests skip explicitly. When enabled,
they refuse any non-localhost emulator endpoint. Tests use the Go Firestore
client directly; the product's development runtime additionally requires both
Auth and Firestore emulators to prevent mixing real and emulated services.

Coverage includes atomic concurrent login rotation, hash-only cookie storage,
revoked-cookie rejection, expiry independent of TTL cleanup, shared bootstrap
budgets, and rotation budgets that survive changing cookie tokens. Google
identity is a deterministic test verifier here. Separate SDK tests reject
malformed/unsigned tokens; a live Google OAuth round-trip is not covered.

Project tests also cover durable publication, full-content deduplication,
six-character hash collisions, unchanged URLs on updates, idempotent retries
after lost responses, owner-only access, claiming/restoration, expiry, concurrent
quota/update reservations, failed installation recovery, and tombstones. The HTTP
integration test combines the actual session store, project repository, staging,
and API handlers. It uses a test Google verifier and does not test public serving.

Deploy `deploy/firestore.indexes.json` before enabling the maintenance query in a
real database. The emulator does not enforce all production index requirements.

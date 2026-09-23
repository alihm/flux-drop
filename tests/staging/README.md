# Final-image integration

Run from the repository root with Docker Compose and Node 20.20+ (CI uses Node 22):

```sh
docker compose -f tests/staging/compose.yaml up --build -d
node tests/staging/run.mjs
docker compose -f tests/staging/compose.yaml down -v
```

The test uses the actual root Dockerfile, production Go entrypoint, Nginx config,
Firestore emulator and Firebase Auth emulator. No authentication-verifier mock is
injected into the app. The Auth emulator exchanges a synthetic Google identity for
its own ID token; this is not a real Google OAuth popup or token-signature test.

The runner verifies staging access, anonymous cookies, CSRF rejection, publication,
deduplication, stable-URL update, rename, login rotation, claim, private unlock,
public transition, process restart with retained data, deletion and logout. It
then stops Firestore to check readiness without losing liveness and stops Nginx
to check supervisor failure/restart. All endpoints are fixed loopback ports.

Request cookies/navigation headers are explicitly supplied because this API test
uses loopback HTTP; the HTTPS browser suite separately verifies browser security.
The emulator database is ephemeral, and the named content volume is disposable.
The app omits publishing/username overrides and derives its public origin from
`FLUX_APP_NAME`, exercising the runtime defaults without touching production.
Always finish with this stack's `down -v` to remove its generated test sites.
Never substitute production Firebase credentials or public endpoints.

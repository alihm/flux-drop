# Production Nginx delivery boundary

Run `docker compose -f tests/delivery/compose.yaml run --build --rm delivery`
from the repository root, then `docker compose -f tests/delivery/compose.yaml down`.

The test runs the unchanged production Nginx configuration and real Go delivery
handler in one non-root, read-only container. Installed content lives in a private
tmpfs readable by the shared Go/Nginx UID. No ports are published, no credentials
are used, and no production data is mounted.

Coverage includes actual file bytes, HEAD, range/206, conditional/304, sandbox and
public CORS headers, internal-route denial, metadata file denial, encoded paths,
and authorization after visibility/status/expiry changes or metadata outages.
Corrupted local content must fail closed.

The authoritative resolver is an in-memory test double. Firestore transactions
have a separate emulator suite. This test does not cover peer fallback, Google
login, private password grants, or publication through the command entrypoint.

The same non-root/read-only fixture also runs TestPrimaryCacheNginx, exercising
actual primary-to-secondary pinned TLS transfer followed by retained-cache nginx
aliases, original-path HTML/JS MIME, HEAD/range/304, branded HTML in Go, private
headers/denial and direct internal-route/traversal rejection. Its cache root is a
disposable node-local tmpfs; no production directories are mounted.

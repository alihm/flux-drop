# Static hosting compatibility: same-domain mode

All uploaded documents execute in an opaque browser origin under an enforced
HTTP Content-Security-Policy sandbox. JavaScript is allowed, but the document
does not receive the origin of Drop's management application. Minification or
obfuscation does not change this boundary.

## Public projects

The browser-isolation suite exercises HTML with inline JavaScript and bundled
React/Vue ES modules. Builds must use relative asset paths or the project's
deployment prefix. Drop does not rewrite application bundles or build source.

Public asset responses may use `Access-Control-Allow-Origin: *` **without**
`Access-Control-Allow-Credentials`. These bytes are already public. This permits
module loading from a sandboxed document without granting access to management
or private project data.

Browser cookies, localStorage, IndexedDB, service workers, and ordinary workers
are not supported in this mode. External APIs must accept non-credentialed
cross-origin access; forms/popups are restricted by the sandbox. Do not advertise
unrestricted framework compatibility, third-party login, or offline/PWA support.

## Private projects: initial restricted profile

For now, private projects support self-contained HTML with inline styles/scripts
and embedded assets. Separate-file module builds are not enabled. Private
responses use `Cross-Origin-Resource-Policy: same-origin`, no permissive CORS,
and no shared caching. This also blocks separate private script/image/font assets
from opaque project documents; that restriction is intentional.

Why: every sandboxed project reports an opaque origin. Reflecting `Origin: null`
with credentialed CORS cannot distinguish the intended project from a malicious
project. Cookie authorization alone also cannot prevent another sandboxed
project from embedding a private script or image the browser can access.

The product must explain this restriction before a user enables privacy and
must not silently turn a functioning multi-file site into a broken private site.
Supporting private multi-file applications requires a separately designed,
project-scoped asset capability scheme or isolated project subdomains. That work
is not complete and must not be simulated with permissive CORS.

## Future wildcard domains

Per-project origins can remove many opaque-origin restrictions once Flux routing
and TLS support them. Host-only management cookies and server authorization are
still required. Existing path URLs remain sandboxed; a URL migration must never
remove isolation while projects still share an origin.

## Verification boundaries

See `tests/browser/README.md` for reproducible tests. Its in-memory authorization
fixture is not the final session implementation. Production release still needs
the same attacks exercised against Firestore authorization and replica fallback.

References:

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Security-Policy/sandbox
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cross-Origin-Resource-Policy
- https://playwright.dev/docs/test-projects

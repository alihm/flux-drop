# Public interface

For fast local UI iteration, run `node ui-dev.mjs` at the repository root and
open <http://127.0.0.1:5173/>. This localhost-only workbench reads the actual
embedded home assets on every request, reloads the browser after edits, and uses
an in-memory mock of the project API and Google sign-in. No Docker, Nginx, npm,
or real credentials are required. The mock is intentionally separate from the
production binary and does not validate real authorization or ZIP extraction.

The Go binary embeds the landing page, project dashboard, management dialog, and
private-project unlock page. CSS and JavaScript are local assets embedded into the
HTML under exact-hash Content Security Policy rules. No third-party fonts,
analytics, or runtime UI packages are loaded. Google sign-in uses a locally built
Firebase browser bundle only when the public Firebase web settings are configured.

The landing page accepts a single HTML file, a ZIP, or a folder/multiple files
with a root `index.html`. It validates file count, total upload size, and the
visible entry point before sending a request. Browser checks are advisory; Go
enforces the actual content and path rules. The file list can be cleared. An XHR
upload shows transfer progress and can be cancelled. An aborted request might
still complete on the server; the same idempotency key is retained for a safe
retry. Once a publish result is confirmed, the publish control stays disabled
until the user starts another selection.

The success state includes the public URL, copy and open actions, and a direct
claim action for anonymous projects. The header and project section are hidden
for a new anonymous session with no projects. Signed-in users see an empty
workspace state. Project cards show URL, visibility, size, creation date when
available, and claim or manage actions as appropriate. Management opens a native
dialog with Content, Access, and Settings sections. Native dialog behavior keeps
focus within the modal and Escape closes it; focus returns to the opener.

Claim is not a management setting. Clicking **Keep this project** starts Google
sign-in when needed and then submits the claim with the current CSRF token and
project revision. Claimed projects have no claim action. A claim URL from an older
publish result focuses the relevant card without changing ownership on its own.

Management actions use the displayed revision in `If-Match`. Stale records require
a list refresh before another mutation. Password fields clear after each attempt.
Deletion requires typing the full project URL name. Private sites currently need
self-contained `index.html` files because separate private assets are blocked by
the delivery policy. The UI states this before enabling private access.

The page includes a compact upload guide and uses plain DOM text for project
names and paths. Links open with `noopener noreferrer`. Passwords, CSRF tokens,
and session credentials are not placed in URLs or persistent browser storage.
Anonymous session failure is reported as the reason publishing is unavailable;
selecting files does not hide that error.

Browser tests exercise upload, retry, claim-after-sign-in, management, and empty
states in Chromium, Firefox, and WebKit with intercepted test APIs. The Go suite
checks the embedded page and CSP. Live Google popup, browser behavior against the
real Flux load balancer, and actual file replication still need deployed
acceptance testing.

The current project API uses browser cookies, Origin and CSRF checks. It is not
an agent credential API and no MCP endpoint exists yet. Do not advertise agent
integration until that separate authentication and API workflow is built.

To rebuild the pinned Firebase client bundle after changing its source:

```sh
cd web
npm ci --ignore-scripts
npm run build
```

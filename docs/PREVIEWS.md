# Project thumbnails and Explore

Published on 2026-10-08 as `alihmahdavi/flux-drop:staging`, registry digest
`sha256:9ffcc23ba708a4abf55f59c8975577c07d8265c453b6b2263b1440335214b9dd`.
Roll out this image to every primary replica. Go race checks, Go vet, browser
checks in Chromium/Firefox/WebKit, real image rendering, sandbox checks, and
primary/secondary startup checks passed.

Primary instances render uploaded sites with headless Chromium controlled by
Playwright. Screenshots are **320 × 180 JPEGs**, captured from a desktop viewport
and compressed at quality 68. Images are capped at 64 KiB. One sample rendering
was 3.7 KiB; actual sizes depend on the page.

Successful uploads and version updates enqueue work without waiting for a
screenshot. A quorum-backed lease prevents different primaries from rendering
the same version concurrently. Bounded background scans recover lost jobs and
backfill existing projects. Failed jobs retry after five minutes; interrupted
leases expire after one minute. Preview failures never undo successful uploads.

Images are stored in `/data/thumbnails`, on the **primary app's replicated
`r:/data` volume**. Syncthing distributes them among the primary replicas; no
secondary stores them. Raft replicates the thumbnail state/version reference and
the recent-project index. Image bytes are separate from Raft records. A primary
awaits an already-generated image's volume replication instead of rendering it
again. Until it arrives, the browser shows a placeholder and retries briefly.
This has the existing deployment's asynchronous Syncthing durability model.
Keep `r:/data|ml:state:/var/lib/drop-cluster` on the primary; no new port or env
variable is required.

The renderer loads only verified files from that project's active version via
an ephemeral loopback capability. Browser scripts never receive the capability.
Application API routes, external URLs, frames, downloads, service workers, and
WebSockets are blocked. The Chromium wrapper restricts filesystem access using
Linux Landlock and blocks network sockets, connection syscalls, process memory
access, and signaling with seccomp. Browser environment variables omit app
credentials. The renderer uses one process per job with software rendering;
temporary profiles and process groups are cleaned up on timeout. Rendering is
limited to one job per primary, 30 seconds, 80 asset requests, and 32 MiB of
downloaded assets. Pages depending on external scripts/fonts/images may have an
incomplete preview. Landlock must be enabled on the Flux host; otherwise
rendering fails closed and leaves a placeholder.

Explore shows up to 24 recently published or updated **claimed, live public** projects.
It rereads current project records through quorum-backed transactions on every
request. Unclaimed, private, expired, and deleted sites are omitted regardless of stale
index entries or images on disk. Owner IDs, storage locations, passwords, and
credentials are absent from responses. Names are rendered as text, and links
are restricted to validated local project URLs. Public sites appear
automatically after claim (or publication while signed in). Claiming updates
gallery eligibility without rerendering the same content. Thumbnail requests check current visibility and version; private
images require a current owner's session. Private visitor access grants do not
grant thumbnail access. Images are served with `Cache-Control: no-store`.

Image storage is bounded to 512 MiB and 10,000 files per replicated copy. Old
images can be discarded at that ceiling; projects remain available and older
thumbnails may become placeholders. The current recent index retains at most
100 candidates. Preview metadata is small; image data never enters Raft.

| Method | Endpoint | Access |
| --- | --- | --- |
| GET | `/api/explore` | Public; current public project cards only |
| GET/HEAD | `/api/projects/{id}/thumbnail?v={digest}` | Public for public sites, owner session for private sites |

These routes and workers exist only on the primary storage role. The thumbnail
response has `X-Drop-Preview: ready` for JPEGs or `pending` for the placeholder.
Old version URLs return 404. The `exploreEnabled` field in `/api/config` controls
the homepage's Explore section and previews in the user's site cards.

Renderer behavior follows [Playwright request routing](https://playwright.dev/docs/network)
and the [Linux Landlock filesystem ABI](https://www.kernel.org/doc/html/v6.8/userspace-api/landlock.html).

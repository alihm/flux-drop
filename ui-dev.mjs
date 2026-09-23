// Local UI workbench. It serves the actual embedded page assets with a small,
// in-memory API so design work does not need Docker, Nginx, or Flux.
import {createHash, randomBytes} from 'node:crypto';
import {watch} from 'node:fs';
import {readFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import {fileURLToPath} from 'node:url';
import {dirname, join} from 'node:path';

const root = dirname(fileURLToPath(import.meta.url));
const ui = join(root, 'internal/httpserver/ui');
const host = '127.0.0.1';
const port = Number(process.argv[2] || 5173);
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('Usage: node ui-dev.mjs [port] (1–65535)');
}

const projects = new Map();
const operations = new Map();
const listeners = new Set();
const sessions = new Map();
const namePattern = /^[a-z0-9](?:[a-z0-9-]{0,46}[a-z0-9])?$/;
const idPattern = /^[a-f0-9]{32}$/;
const maxUpload = 50 * 1048576;

function json(res, status, value) {
  const body = JSON.stringify(value);
  res.writeHead(status, {'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(body), 'Cache-Control': 'no-store'});
  res.end(body);
}

function failure(res, status, code) { json(res, status, {error: code}); }

function previewSession(req, res, create = false) {
  const id = (req.headers.cookie || '').split(';').map(value => value.trim()).find(value => value.startsWith('drop-ui-preview='))?.slice(16);
  if (id && sessions.has(id)) return sessions.get(id);
  if (!create) return null;
  const nextID = randomBytes(24).toString('base64url');
  const next = {csrfToken: randomBytes(24).toString('base64url'), authenticated: false, reauthenticationRequired: false};
  Object.defineProperties(next, {id: {value: nextID}, anonymousOwner: {value: randomBytes(24).toString('base64url'), writable: true}});
  sessions.set(nextID, next);
  res.setHeader('Set-Cookie', `drop-ui-preview=${nextID}; Path=/; HttpOnly; SameSite=Lax`);
  return next;
}

async function bodyBytes(req, limit = 60 * 1048576) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw Object.assign(new Error('request_too_large'), {status: 413});
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function bodyJSON(req) {
  const bytes = await bodyBytes(req, 4096);
  try { return JSON.parse(bytes.toString('utf8')); }
  catch { throw Object.assign(new Error('invalid_json'), {status: 400}); }
}

async function bodyFiles(req) {
  const bytes = await bodyBytes(req);
  const contentType = req.headers['content-type'] || '';
  if (!contentType.startsWith('multipart/form-data;')) throw Object.assign(new Error('multipart_required'), {status: 400});
  const form = await new Request('http://localhost/', {method: 'POST', headers: {'Content-Type': contentType}, body: bytes}).formData();
  const parts = form.getAll('files').filter(item => typeof item !== 'string');
  if (!parts.length || parts.length > 5000 || parts.reduce((sum, file) => sum + file.size, 0) > maxUpload) {
    throw Object.assign(new Error('invalid_files'), {status: 400});
  }
  const files = [];
  for (const part of parts) files.push({name: part.name, bytes: Buffer.from(await part.arrayBuffer())});
  return files;
}

function normalizedFiles(files) {
  if (files.length === 1 && /\.zip$/i.test(files[0].name)) {
    return new Map([['index.html', Buffer.from('<!doctype html><title>ZIP preview</title><h1>ZIP uploaded</h1><p>This UI workbench does not extract ZIP files. The production service does.</p>')]]);
  }
  if (files.length === 1 && /\.html?$/i.test(files[0].name)) return new Map([['index.html', files[0].bytes]]);
  const names = files.map(file => file.name.replaceAll('\\', '/'));
  const first = names[0].split('/')[0];
  const strip = first && names.every(name => name.startsWith(first + '/')) ? first.length + 1 : 0;
  const result = new Map();
  for (let i = 0; i < files.length; i++) {
    const path = names[i].slice(strip);
    if (!path || path.startsWith('/') || path.split('/').some(part => !part || part === '.' || part === '..')) continue;
    result.set(path, files[i].bytes);
  }
  if (!result.has('index.html')) {
    result.set('index.html', Buffer.from('<!doctype html><title>Site preview</title><h1>Project uploaded</h1><p>The UI workbench could not find a root index.html. Check the folder paths in your upload.</p>'));
  }
  return result;
}

function digest(files) {
  const hash = createHash('sha256');
  for (const file of files) { hash.update(file.name); hash.update('\0'); hash.update(file.bytes); }
  return hash.digest('hex');
}

function projectJSON(project) {
  const {files, digest: _, anonymousOwner, accountOwner, ...publicProject} = project;
  return publicProject;
}

function canManage(project, viewer) {
  return Boolean(viewer && (project.anonymousOwner === viewer.anonymousOwner || viewer.authenticated && project.accountOwner === viewer.id));
}

function changed(project) {
  project.revision++;
  return {project: projectJSON(project), path: '/' + project.slug + '/'};
}

function matchingProject(req, res, id, viewer) {
  if (!idPattern.test(id) || !projects.has(id)) { failure(res, 404, 'not_found'); return null; }
  const project = projects.get(id);
  if (!canManage(project, viewer)) { failure(res, 404, 'not_found'); return null; }
  if (req.headers['if-match'] !== `"${project.revision}"`) { failure(res, 409, 'revision_conflict'); return null; }
  return project;
}

function mime(path) {
  const ext = path.split('.').at(-1).toLowerCase();
  return ({html: 'text/html; charset=utf-8', htm: 'text/html; charset=utf-8', css: 'text/css; charset=utf-8', js: 'text/javascript; charset=utf-8', svg: 'image/svg+xml', png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', webp: 'image/webp'})[ext] || 'application/octet-stream';
}

async function home(res) {
  const [html, css, js] = await Promise.all(['home.html', 'home.css', 'home.js'].map(name => readFile(join(ui, name), 'utf8')));
  const mockAuth = `window.DropAuth ||= {preview:true,init(){},async token(){return 'ui-preview-token'},async clear(){}};`;
  const reload = `new EventSource('/__ui/reload').onmessage=()=>location.reload();`;
  const page = html.replace('{{.CSS}}', css).replace('{{.JS}}', mockAuth + '\n' + js + '\n' + reload);
  res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store'});
  res.end(page);
}

async function handle(req, res) {
  const url = new URL(req.url, `http://${host}:${port}`);
  const path = url.pathname;
  if (path === '/' && req.method === 'GET') return home(res);
  if (path === '/__ui/reload' && req.method === 'GET') {
    res.writeHead(200, {'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive'});
    res.write(': connected\n\n');
    listeners.add(res);
    req.on('close', () => listeners.delete(res));
    return;
  }
  if (path === '/api/config' && req.method === 'GET') return json(res, 200, {
    publicOrigin: `http://${host}:${port}`, publishingEnabled: true, authenticationEnabled: true,
    limits: {uploadBytes: maxUpload, expandedBytes: 200 * 1048576, files: 5000},
    firebase: {projectId: 'ui-preview'}
  });
  if (path === '/api/session' && req.method === 'POST') return json(res, 200, previewSession(req, res, true));
  const viewer = previewSession(req, res);
  if (path === '/api/auth/google' && req.method === 'POST') {
    if (!viewer || req.headers['x-csrf-token'] !== viewer.csrfToken) return failure(res, 403, 'invalid_session');
    viewer.authenticated = true;
    viewer.csrfToken = randomBytes(24).toString('base64url');
    return json(res, 200, viewer);
  }
  if (path === '/api/auth/logout' && req.method === 'POST') {
    if (!viewer || req.headers['x-csrf-token'] !== viewer.csrfToken) return failure(res, 403, 'invalid_session');
    viewer.authenticated = false;
    viewer.anonymousOwner = randomBytes(24).toString('base64url');
    viewer.csrfToken = randomBytes(24).toString('base64url');
    return json(res, 200, viewer);
  }
  if (path === '/api/projects' && req.method === 'GET') {
    return json(res, 200, {projects: [...projects.values()].filter(project => canManage(project, viewer)).map(projectJSON), nextCursor: ''});
  }
  if (path === '/api/projects' && req.method === 'POST') {
    if (!viewer) return failure(res, 401, 'invalid_session');
    const name = url.searchParams.get('name') || '';
    if (!namePattern.test(name)) return failure(res, 400, 'invalid_name');
    const secret = req.headers['x-drop-password'];
    if (secret !== undefined && [...Buffer.from(secret, 'base64url').toString('utf8')].length < 12) return failure(res, 400, 'invalid_password');
    const files = await bodyFiles(req);
    const operation = req.headers['idempotency-key'];
    if (operation && operations.has(operation)) return json(res, 200, operations.get(operation));
    const fullDigest = digest(files);
    const existing = [...projects.values()].find(project => project.digest === fullDigest);
    if (existing) return json(res, 409, {error: 'duplicate_content', path: '/' + existing.slug + '/'});
    const slug = `${name}-${fullDigest.slice(0, 6)}`;
    if ([...projects.values()].some(project => project.slug === slug)) return failure(res, 409, 'name_conflict');
    const project = {
      id: randomBytes(16).toString('hex'), slug, revision: 1, private: secret !== undefined,
      createdAt: new Date().toISOString(), expiresAt: viewer?.authenticated ? null : new Date(Date.now() + 30 * 86400000).toISOString(),
      bytes: files.reduce((sum, file) => sum + file.bytes.length, 0),
      files: normalizedFiles(files), digest: fullDigest,
      anonymousOwner: viewer.authenticated ? null : viewer.anonymousOwner,
      accountOwner: viewer.authenticated ? viewer.id : null
    };
    projects.set(project.id, project);
    const result = {project: projectJSON(project), path: '/' + slug + '/'};
    if (operation) operations.set(operation, result);
    return json(res, 200, result);
  }
  const projectRoute = /^\/api\/projects\/([a-f0-9]{32})(?:\/(claim|privacy|versions))?$/.exec(path);
  if (projectRoute) {
    const [, id, action] = projectRoute;
    if (!action && req.method === 'GET') {
      const project = projects.get(id);
      return project && canManage(project, viewer) ? json(res, 200, {project: projectJSON(project), path: '/' + project.slug + '/'}) : failure(res, 404, 'not_found');
    }
    const project = matchingProject(req, res, id, viewer);
    if (!project) return;
    if (action === 'claim' && req.method === 'POST') {
      if (!viewer?.authenticated) return failure(res, 403, 'sign_in_required');
      project.expiresAt = null;
      project.anonymousOwner = null;
      project.accountOwner = viewer.id;
      return json(res, 200, changed(project));
    }
    if (action === 'privacy' && req.method === 'PUT') {
      const input = await bodyJSON(req);
      project.private = Boolean(input.private);
      return json(res, 200, changed(project));
    }
    if (action === 'versions' && req.method === 'POST') {
      const files = await bodyFiles(req);
      project.files = normalizedFiles(files);
      project.digest = digest(files);
      project.bytes = files.reduce((sum, file) => sum + file.bytes.length, 0);
      return json(res, 200, changed(project));
    }
    if (!action && req.method === 'PATCH') {
      const input = await bodyJSON(req);
      if (!namePattern.test(input.name)) return failure(res, 400, 'invalid_name');
      project.slug = input.name + '-' + project.slug.slice(-6);
      return json(res, 200, changed(project));
    }
    if (!action && req.method === 'DELETE') {
      projects.delete(id);
      res.writeHead(204);
      return res.end();
    }
  }
  const slash = path.indexOf('/', 1);
  const slug = slash < 0 ? path.slice(1) : path.slice(1, slash);
  const project = [...projects.values()].find(item => item.slug === slug);
  if (project && req.method === 'GET') {
    if (project.private) {
      res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store'});
      return res.end('<!doctype html><title>Private preview</title><h1>Private project</h1><p>The local UI workbench does not simulate the password unlock page.</p>');
    }
    let asset;
    try { asset = decodeURIComponent(path.slice(slug.length + 2)) || 'index.html'; }
    catch { return failure(res, 400, 'invalid_path'); }
    if (asset.split('/').some(part => part === '..' || part === '.')) return failure(res, 400, 'invalid_path');
    const bytes = project.files.get(asset);
    if (!bytes) return failure(res, 404, 'not_found');
    res.writeHead(200, {'Content-Type': mime(asset), 'Cache-Control': 'no-store', 'Content-Security-Policy': "sandbox allow-scripts; object-src 'none'; base-uri 'none'"});
    return res.end(bytes);
  }
  failure(res, 404, 'not_found');
}

const server = createServer((req, res) => {
  handle(req, res).catch(error => failure(res, error.status || 500, error.message || 'preview_error'));
});
let reloadTimer;
const watcher = watch(ui, (_event, filename) => {
  if (!['home.html', 'home.css', 'home.js'].includes(String(filename))) return;
  clearTimeout(reloadTimer);
  reloadTimer = setTimeout(() => {
    for (const client of listeners) client.write('data: reload\n\n');
  }, 120);
});
server.listen(port, host, () => console.log(`Flux Drop UI preview: http://${host}:${port}\nEdit internal/httpserver/ui/{home.html,home.css,home.js} and the page reloads.`));
let stopping = false;
function stop() {
  if (stopping) return;
  stopping = true;
  clearTimeout(reloadTimer);
  watcher.close();
  for (const client of listeners) client.end();
  listeners.clear();
  server.close();
  server.closeAllConnections();
}
process.on('SIGINT', stop);
process.on('SIGTERM', stop);

"use strict";
(() => {
  const $ = id => document.getElementById(id);
  const namePattern = /^[a-z0-9](?:[a-z0-9-]{0,46}[a-z0-9])?$/;
  const pathPattern = /^\/[a-z0-9][a-z0-9-]*-[a-f0-9]{6}\/$/;
  const names = ['quiet-orbit', 'bright-signal', 'tiny-comet', 'fresh-horizon', 'silver-cloud', 'open-canvas'];
  const randomName = () => {
    const value = new Uint32Array(1);
    crypto.getRandomValues(value);
    return names[value[0] % names.length];
  };
  const dialog = $('manage-dialog');
  let config, session, files = [], selectionError = '', key = '', cursor = '';
  let uploading = false, authenticating = false, managing = false, published = false;
  let currentUpload, lastPublishedProject, modalTrigger;

  $('name').value = randomName();

  function status(message, isError = false) {
    $('status').textContent = message;
    $('status').classList.toggle('error', isError);
  }

  function setAuthState() {
    $('sign-in').hidden = !config?.firebase || !window.DropAuth || Boolean(session?.authenticated);
    $('sign-out').hidden = !session?.authenticated && !session?.reauthenticationRequired;
    $('sign-in').disabled = authenticating || !session;
    $('sign-out').disabled = authenticating || !session;
  }

  function state() {
    const nameValid = namePattern.test($('name').value);
    $('name').setAttribute('aria-invalid', String(!nameValid));
    $('name-help').textContent = nameValid
      ? 'Lowercase letters, numbers, and hyphens. A unique suffix is added to the URL.'
      : 'Use 1–48 lowercase letters, numbers, or internal hyphens.';
    $('choose-files').disabled = uploading;
    $('choose-folder').disabled = uploading;
    $('name').disabled = uploading;
    $('publish').disabled = uploading || authenticating || published || !config?.publishingEnabled || !session || !files.length || Boolean(selectionError) || !nameValid;
    $('clear-selection').disabled = uploading;
    setAuthState();
  }

  function selectionProblem(selected) {
    if (!config) return 'The service is still connecting. Try again in a moment.';
    if (selected.length > config.limits.files) return `Choose no more than ${config.limits.files.toLocaleString()} files.`;
    if (selected.reduce((sum, file) => sum + file.size, 0) > config.limits.uploadBytes) return 'This selection exceeds the upload limit. Choose a smaller site.';
    if (!selected.length) return '';
    if (selected.length === 1 && /\.(html?|zip)$/i.test(selected[0].name)) return '';
    const paths = selected.map(file => file.webkitRelativePath || file.name);
    if (paths.includes('index.html')) return '';
    const roots = new Set(paths.map(path => path.split('/')[0]));
    if (roots.size === 1 && paths.includes([...roots][0] + '/index.html')) return '';
    return 'Include an index.html at the site root, or choose one HTML or ZIP file.';
  }

  function renderSelection() {
    const total = files.reduce((sum, file) => sum + file.size, 0);
    $('selection').textContent = files.length
      ? `${files.length} file${files.length === 1 ? '' : 's'} · ${(total / 1048576).toFixed(2)} MiB`
      : 'No files selected';
    $('clear-selection').hidden = !files.length;
    const list = $('selected-files');
    list.replaceChildren();
    for (const file of files.slice(0, 5)) {
      const item = document.createElement('li');
      item.textContent = file.webkitRelativePath || file.name;
      list.append(item);
    }
    if (files.length > 5) {
      const item = document.createElement('li');
      item.textContent = `…and ${files.length - 5} more`;
      list.append(item);
    }
    list.hidden = !files.length;
    state();
  }

  function clearSelection() {
    files = [];
    selectionError = '';
    key = '';
    $('files').value = '';
    $('folder').value = '';
    renderSelection();
  }

  function choose(selected) {
    if (uploading) return;
    files = selected;
    selectionError = selectionProblem(files);
    key = crypto.randomUUID();
    published = false;
    lastPublishedProject = null;
    $('result').hidden = true;
    renderSelection();
    if (selectionError) status(selectionError, true);
    else if (!session) status('A browser session is unavailable. Refresh this page before publishing.', true);
    else if (!config.publishingEnabled) status('Publishing is unavailable right now.', true);
    else status(files.length ? 'Ready to publish.' : 'Choose a site to get started.');
  }

  $('choose-files').onclick = () => $('files').click();
  $('choose-folder').onclick = () => $('folder').click();
  $('files').onchange = event => choose([...event.target.files]);
  $('folder').onchange = event => choose([...event.target.files]);
  $('clear-selection').onclick = () => { clearSelection(); status('Choose a site to get started.'); };
  $('name').oninput = () => { key = crypto.randomUUID(); state(); };

  const zone = $('dropzone');
  for (const eventName of ['dragenter', 'dragover']) {
    zone.addEventListener(eventName, event => { event.preventDefault(); zone.classList.add('drag'); });
  }
  zone.addEventListener('dragleave', event => { if (!zone.contains(event.relatedTarget)) zone.classList.remove('drag'); });
  zone.addEventListener('drop', event => {
    event.preventDefault();
    zone.classList.remove('drag');
    const items = [...event.dataTransfer.items];
    if (items.some(item => item.webkitGetAsEntry?.()?.isDirectory)) {
      status('To keep folder paths intact, use Choose folder.', true);
      return;
    }
    choose([...event.dataTransfer.files]);
  });

  function upload(url, body, headers, onProgress) {
    return new Promise((resolve, reject) => {
      const request = new XMLHttpRequest();
      currentUpload = request;
      request.open('POST', url);
      for (const [name, value] of Object.entries(headers)) request.setRequestHeader(name, value);
      request.upload.onprogress = event => {
        if (event.lengthComputable) onProgress(Math.min(100, Math.round(event.loaded * 100 / event.total)));
      };
      request.onload = () => {
        currentUpload = null;
        let data;
        try { data = JSON.parse(request.responseText); }
        catch { reject(new Error('The server returned an unexpected response. Check your projects before retrying.')); return; }
        resolve({status: request.status, data});
      };
      request.onerror = () => { currentUpload = null; reject(new TypeError('Network error')); };
      request.onabort = () => { currentUpload = null; reject(new DOMException('Upload cancelled', 'AbortError')); };
      request.send(body);
    });
  }

  $('cancel-upload').onclick = () => currentUpload?.abort();
  $('publish-another').onclick = () => {
    clearSelection();
    published = false;
    lastPublishedProject = null;
    $('name').value = randomName();
    $('result').hidden = true;
    status('Choose a site to get started.');
    state();
    zone.scrollIntoView({behavior: 'smooth', block: 'center'});
  };

  async function copyLink(url, button, message) {
    try {
      await navigator.clipboard.writeText(url);
      const original = button.textContent;
      button.textContent = 'Copied';
      setTimeout(() => { button.textContent = original; }, 2000);
    } catch { message.textContent = 'Copy was blocked by your browser. Open the site and copy its address.'; }
  }
  $('copy-result').onclick = () => copyLink($('project-link').href, $('copy-result'), $('status'));

  function publishError(code, data) {
    if (code === 413) return 'The upload is too large, including ZIP or form packaging.';
    if (code === 429) return 'Too many requests. Wait a moment and retry.';
    if (code === 409) return data.error === 'name_conflict' ? 'That name is in use. Choose another.' : 'The name, quota, or project state conflicts. Check your projects before retrying.';
    if (code === 400) return 'Check your files and project name, then retry.';
    return 'Publishing could not be confirmed. Retry this selection safely, or check your projects.';
  }

  $('publish').onclick = async () => {
    if ($('publish').disabled) return;
    uploading = true;
    state();
    $('upload-progress').hidden = false;
    $('progress-fill').style.width = '0%';
    $('progress-label').textContent = 'Uploading…';
    status('Uploading and verifying your files…');
    try {
      const body = new FormData();
      for (const file of files) body.append('files', file, file.webkitRelativePath || file.name);
      const response = await upload('/api/projects?name=' + encodeURIComponent($('name').value), body,
        {'X-CSRF-Token': session.csrfToken, 'Idempotency-Key': key}, percent => {
          $('progress-fill').style.width = percent + '%';
          $('progress-label').textContent = percent === 100 ? 'Upload complete. Publishing…' : `${percent}% uploaded`;
        });
      const {status: code, data} = response;
      if (code < 200 || code >= 300) {
        if (!(code === 409 && data.error === 'duplicate_content')) throw new Error(publishError(code, data));
      }
      if (typeof data.path !== 'string' || !pathPattern.test(data.path)) throw new Error('The server returned an invalid project link.');
      published = true;
      lastPublishedProject = code < 300 && data.project?.expiresAt && /^[a-f0-9]{32}$/.test(data.project?.id) ? data.project : null;
      $('result-label').textContent = code < 300 ? 'Your site is live.' : 'These files are already published.';
      $('project-link').href = data.path;
      $('project-link').textContent = window.location.origin + data.path;
      $('claim-result').hidden = !lastPublishedProject;
      $('expiry').textContent = lastPublishedProject
        ? `Expires ${new Date(lastPublishedProject.expiresAt).toLocaleDateString()}. Claim it to keep it and manage it on another device.`
        : '';
      $('result').hidden = false;
      status(code < 300 ? 'Published successfully.' : 'No duplicate project was created.');
      await listProjects();
    } catch (error) {
      status(error.name === 'AbortError'
        ? 'Upload stopped. It may still complete on the server; retry the same selection or check your projects.'
        : error instanceof TypeError ? 'Connection interrupted. Retry the same selection safely.' : error.message, true);
    } finally {
      uploading = false;
      $('upload-progress').hidden = true;
      state();
    }
  };

  async function exchange(path, body) {
    const response = await fetch(path, {method: 'POST', headers: {'Content-Type': 'application/json', 'X-CSRF-Token': session.csrfToken}, body: JSON.stringify(body)});
    if (!response.ok) throw new Error(response.status === 409 ? 'Sign out before switching Google accounts.' : response.status === 429 ? 'Too many sign-in attempts. Wait and try again.' : 'Session change could not be confirmed. Refresh this page before retrying.');
    const next = await response.json();
    if (!next.csrfToken) throw new Error('Invalid session response. Refresh this page.');
    session = next;
    state();
  }

  async function signIn(refresh = true) {
    if (authenticating || uploading || managing || !session || !window.DropAuth) return false;
    authenticating = true;
    state();
    $('auth-status').textContent = 'Complete sign-in in the Google window.';
    try {
      // Open the popup in the click handler's user gesture, before any network await.
      const idToken = await window.DropAuth.token();
      await exchange('/api/auth/google', {idToken});
      $('auth-status').textContent = 'Signed in. Claimed projects stay available across devices.';
      if (refresh) await listProjects();
      return true;
    } catch (error) {
      $('auth-status').textContent = error.code === 'auth/popup-closed-by-user' ? 'Sign-in cancelled.'
        : error.code === 'auth/popup-blocked' ? 'Allow the sign-in popup and try again.' : error.message;
      return false;
    } finally { authenticating = false; state(); }
  }
  $('sign-in').onclick = () => signIn();
  $('sign-out').onclick = async () => {
    if (authenticating || uploading || managing || !session) return;
    if (!window.confirm('Sign out? Unclaimed projects in this browser will no longer be available. Claim them first to keep access.')) return;
    authenticating = true;
    state();
    try {
      await exchange('/api/auth/logout', {});
      await window.DropAuth?.clear();
      $('auth-status').textContent = 'Signed out. A new anonymous session is active.';
      await listProjects();
    } catch (error) { $('auth-status').textContent = error.message; }
    finally { authenticating = false; state(); }
  };

  function projectPath(project) {
    const path = '/' + project.slug + '/';
    return pathPattern.test(path) ? path : null;
  }

  function addButton(parent, label, className, action) {
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = label;
    button.className = className;
    button.onclick = action;
    parent.append(button);
    return button;
  }

  async function claimProject(project, message) {
    if (authenticating || managing || uploading || !session || !project.expiresAt) return;
    if (!session.authenticated && !await signIn(false)) {
      message.textContent = 'Sign in with Google to finish claiming this project.';
      return;
    }
    managing = true;
    message.textContent = 'Claiming project…';
    try {
      const response = await fetch('/api/projects/' + project.id + '/claim', {
        method: 'POST', headers: {'Content-Type': 'application/json', 'X-CSRF-Token': session.csrfToken, 'If-Match': `"${project.revision}"`}, body: '{}'
      });
      if (!response.ok) throw new Error(response.status === 409 || response.status === 404
        ? 'This project changed. Refresh your projects, then claim it.'
        : 'Could not confirm the claim. Refresh your projects before retrying.');
      if (lastPublishedProject?.id === project.id) {
        lastPublishedProject = null;
        $('claim-result').hidden = true;
        $('expiry').textContent = 'Claimed. This project no longer expires.';
      }
      message.textContent = 'Claimed. This project no longer expires.';
      await listProjects();
    } catch (error) { message.textContent = error.message; }
    finally { managing = false; }
  }
  $('claim-result').onclick = () => {
    if (lastPublishedProject) claimProject(lastPublishedProject, $('expiry'));
  };

  function dateLabel(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '' : date.toLocaleDateString();
  }
  function bytesLabel(value) {
    if (!Number.isFinite(value) || value < 0) return '';
    return value < 1048576 ? `${Math.ceil(value / 1024)} KiB` : `${(value / 1048576).toFixed(1)} MiB`;
  }

  function projectCard(project) {
    const path = projectPath(project);
    if (!path || !/^[a-f0-9]{32}$/.test(project.id) || !Number.isSafeInteger(project.revision) || project.revision < 1) return null;
    const card = document.createElement('article');
    card.className = 'project-card';
    card.dataset.projectId = project.id;
    const top = document.createElement('div'); top.className = 'card-top';
    const heading = document.createElement('h3');
    const link = document.createElement('a'); link.href = path; link.textContent = project.slug; link.target = '_blank'; link.rel = 'noopener noreferrer'; heading.append(link);
    const badge = document.createElement('span'); badge.className = 'badge' + (project.private ? ' private' : ''); badge.textContent = project.private ? 'Private' : 'Public';
    top.append(heading, badge); card.append(top);
    const url = document.createElement('p'); url.className = 'card-url'; url.textContent = window.location.host + path; card.append(url);
    const meta = document.createElement('div'); meta.className = 'card-meta';
    for (const label of [project.expiresAt ? `Expires ${dateLabel(project.expiresAt)}` : 'Claimed · No expiry', project.createdAt ? `Created ${dateLabel(project.createdAt)}` : '', bytesLabel(project.bytes)]) {
      if (!label) continue;
      const span = document.createElement('span'); span.textContent = label;
      if (project.expiresAt && label.startsWith('Expires')) span.className = 'expiring';
      meta.append(span);
    }
    card.append(meta);
    const actions = document.createElement('div'); actions.className = 'card-actions';
    const open = document.createElement('a'); open.href = path; open.target = '_blank'; open.rel = 'noopener noreferrer'; open.textContent = 'Open site ↗'; actions.append(open);
    const message = document.createElement('p'); message.className = 'card-status'; message.setAttribute('role', 'status');
    addButton(actions, 'Copy link', 'secondary', event => copyLink(new URL(path, window.location.origin).href, event.currentTarget, message));
    if (project.expiresAt) addButton(actions, 'Keep this project', 'claim', () => claimProject(project, message));
    addButton(actions, 'Manage', 'secondary', event => openManager(project, event.currentTarget));
    card.append(actions, message);
    return card;
  }

  async function listProjects(append = false) {
    if (!session) return;
    $('refresh-projects').disabled = true;
    $('more-projects').disabled = true;
    try {
      const response = await fetch('/api/projects' + (append && cursor ? '?cursor=' + encodeURIComponent(cursor) : ''), {cache: 'no-store'});
      if (!response.ok) throw new Error('Could not load projects. Refresh to try again.');
      const data = await response.json();
      if (!append) $('project-list').replaceChildren();
      for (const project of data.projects || []) {
        const card = projectCard(project);
        if (card) $('project-list').append(card);
      }
      cursor = data.nextCursor || '';
      const count = $('project-list').children.length;
      $('projects-nav').hidden = count === 0;
      $('project-count').textContent = count + (cursor ? '+' : '');
      $('projects').hidden = count === 0 && !session.authenticated;
      $('more-projects').hidden = !cursor;
      $('projects-status').textContent = count
        ? `${count}${cursor ? '+' : ''} project${count === 1 ? '' : 's'} available to this session.`
        : session.authenticated ? 'No projects yet. Publish your first site above.' : '';
      const claimID = new URLSearchParams(window.location.search).get('claim');
      if (claimID && !append) {
        const card = [...$('project-list').children].find(item => item.dataset.projectId === claimID && item.querySelector('.claim'));
        if (card) { card.scrollIntoView({block: 'center'}); card.querySelector('.claim').focus({preventScroll: true}); }
        history.replaceState(null, '', window.location.pathname + window.location.hash);
      }
    } catch (error) {
      $('projects').hidden = false;
      $('projects-status').textContent = error.message;
    } finally {
      $('refresh-projects').disabled = false;
      $('more-projects').disabled = false;
    }
  }
  $('refresh-projects').onclick = () => listProjects();
  $('more-projects').onclick = () => listProjects(true);

  function field(parent, labelText, type, value = '') {
    const label = document.createElement('label'); label.textContent = labelText;
    const input = document.createElement('input'); input.type = type; input.value = value;
    label.append(input); parent.append(label);
    return input;
  }
  function note(parent, text) { const p = document.createElement('p'); p.textContent = text; parent.append(p); return p; }

  function openManager(project, trigger) {
    if (managing) return;
    modalTrigger = trigger;
    $('manage-title').textContent = project.slug;
    $('manage-url').textContent = window.location.origin + '/' + project.slug + '/';
    const content = $('manage-content'); content.replaceChildren();
    const tabs = document.createElement('div'); tabs.className = 'manage-tabs';
    const panels = document.createElement('div');
    const tabItems = [];
    for (const [name, title] of [['content', 'Content'], ['access', 'Access'], ['settings', 'Settings']]) {
      const panel = document.createElement('section'); panel.className = 'manage-panel'; panel.hidden = name !== 'content';
      const tab = addButton(tabs, title, '', () => {
        for (const item of tabItems) { item.panel.hidden = item.name !== name; item.tab.setAttribute('aria-pressed', String(item.name === name)); }
      });
      tab.setAttribute('aria-pressed', String(name === 'content'));
      tabItems.push({name, tab, panel}); panels.append(panel);
    }
    content.append(tabs, panels);
    const [contentPanel, accessPanel, settingsPanel] = tabItems.map(item => item.panel);
    const message = document.createElement('p'); message.className = 'management-status'; message.setAttribute('role', 'status'); content.append(message);
    const passwordInputs = [];
    const setMessage = (text, error = false) => { message.textContent = text; message.classList.toggle('error', error); };
    const perform = async (method, suffix, body, extra = {}) => {
      if (managing || authenticating) return;
      managing = true;
      let stale = false;
      for (const button of content.querySelectorAll('.manage-panel button')) button.disabled = true;
      $('close-dialog').disabled = true;
      setMessage('Saving…');
      try {
        const headers = {'X-CSRF-Token': session.csrfToken, 'If-Match': `"${project.revision}"`, ...extra};
        let response;
        if (body instanceof FormData) {
          response = await upload('/api/projects/' + project.id + suffix, body, headers, percent => setMessage(percent === 100 ? 'Upload complete. Saving…' : `${percent}% uploaded…`));
        } else {
          headers['Content-Type'] = 'application/json';
          const result = await fetch('/api/projects/' + project.id + suffix, {method, headers, body: body === undefined ? undefined : JSON.stringify(body)});
          response = {status: result.status, data: result.status === 204 ? {} : await result.json()};
        }
        if (response.status < 200 || response.status >= 300) {
          if (response.status === 409 || response.status === 404) { stale = true; throw new Error('This project changed. Close settings and refresh your projects.'); }
          throw new Error(response.status === 400 ? 'Check your input and try again.' : 'The result could not be confirmed. Refresh projects before retrying.');
        }
        dialog.close();
        await listProjects();
        status('Project updated.');
      } catch (error) {
        setMessage(error instanceof TypeError ? 'Connection interrupted. Refresh projects to check whether the change completed.' : error.message, true);
      } finally {
        for (const input of passwordInputs) input.value = '';
        managing = false;
        $('close-dialog').disabled = false;
        for (const button of content.querySelectorAll('.manage-panel button')) button.disabled = stale;
      }
    };

    const contentTitle = document.createElement('h3'); contentTitle.textContent = 'Replace site files'; contentPanel.append(contentTitle);
    note(contentPanel, 'Upload a new HTML file, ZIP, or folder. Your URL and original expiry stay the same.');
    const replacementFiles = field(contentPanel, 'Replacement HTML, ZIP, or files', 'file'); replacementFiles.multiple = true;
    const replacementFolder = field(contentPanel, 'Or choose a replacement folder', 'file'); replacementFolder.multiple = true; replacementFolder.setAttribute('webkitdirectory', '');
    let replacement = [], replacementKey = '';
    const selectReplacement = event => {
      replacement = [...event.target.files]; replacementKey = crypto.randomUUID();
      setMessage(`${replacement.length} replacement file${replacement.length === 1 ? '' : 's'} selected. Current content is unchanged until saved.`);
      if (event.target === replacementFiles) replacementFolder.value = ''; else replacementFiles.value = '';
    };
    replacementFiles.onchange = selectReplacement; replacementFolder.onchange = selectReplacement;
    addButton(contentPanel, 'Replace content', 'secondary', () => {
      const problem = selectionProblem(replacement);
      if (!replacement.length || problem) { setMessage(problem || 'Choose replacement files first.', true); return; }
      if (!window.confirm('Replace this project’s current content? Its URL will stay the same.')) return;
      const body = new FormData();
      for (const file of replacement) body.append('files', file, file.webkitRelativePath || file.name);
      perform('POST', '/versions', body, {'Idempotency-Key': replacementKey});
    });

    const accessTitle = document.createElement('h3'); accessTitle.textContent = project.private ? 'Private access' : 'Public access'; accessPanel.append(accessTitle);
    note(accessPanel, project.private ? 'Visitors need the project password to open this site.' : 'Anyone with this URL can view the site.');
    const password = field(accessPanel, project.private ? 'New project password' : 'Password for private access', 'password');
    password.autocomplete = 'new-password'; password.maxLength = 1024; passwordInputs.push(password);
    note(accessPanel, 'Use at least 12 characters. Private projects currently support only self-contained index.html pages, without separate assets.');
    addButton(accessPanel, project.private ? 'Change password' : 'Make private', 'secondary', () => {
      if ([...password.value].length < 12 || new TextEncoder().encode(password.value).length > 1024) { setMessage('Use at least 12 characters and at most 1,024 UTF-8 bytes.', true); return; }
      perform('PUT', '/privacy', {private: true, password: password.value});
    });
    if (project.private) addButton(accessPanel, 'Make public', 'secondary', () => {
      if (window.confirm('Make this project public? Anyone with its link will be able to view it.')) perform('PUT', '/privacy', {private: false});
    });

    const settingsTitle = document.createElement('h3'); settingsTitle.textContent = 'Project name'; settingsPanel.append(settingsTitle);
    note(settingsPanel, 'Renaming keeps the six-character suffix. Existing links may continue to redirect.');
    const rename = field(settingsPanel, 'New project name', 'text', project.slug.slice(0, -7)); rename.maxLength = 48;
    addButton(settingsPanel, 'Rename', 'secondary', () => {
      if (!namePattern.test(rename.value)) { setMessage('Use 1–48 lowercase letters, numbers, or internal hyphens.', true); return; }
      perform('PATCH', '', {name: rename.value});
    });
    const deleteTitle = document.createElement('h3'); deleteTitle.textContent = 'Delete project'; deleteTitle.style.marginTop = '34px'; settingsPanel.append(deleteTitle);
    note(settingsPanel, 'Deleting removes the public URL and project access. This cannot be undone.');
    const deletion = field(settingsPanel, 'Type the full project URL name to confirm', 'text'); deletion.autocomplete = 'off'; deletion.spellcheck = false;
    addButton(settingsPanel, 'Delete project', 'danger', () => {
      if (deletion.value !== project.slug) { setMessage('Enter the exact project URL name to confirm deletion.', true); return; }
      perform('DELETE', '');
    });

    dialog.showModal();
    $('close-dialog').focus();
  }
  $('close-dialog').onclick = () => { if (!managing) dialog.close(); };
  dialog.addEventListener('cancel', event => { if (managing) event.preventDefault(); });
  dialog.addEventListener('close', () => { if (modalTrigger?.isConnected) modalTrigger.focus(); modalTrigger = null; });

  (async () => {
    state();
    try {
      const response = await fetch('/api/config', {cache: 'no-store'});
      if (!response.ok) throw new Error('Service configuration is unavailable. Refresh this page to retry.');
      config = await response.json();
      if (config.firebase && window.DropAuth) window.DropAuth.init(config.firebase);
      $('limits').textContent = `${Math.floor(config.limits.uploadBytes / 1048576)} MiB upload · ${config.limits.files.toLocaleString()} files`;
      state();
      if (config.authenticationEnabled) {
        const bootstrap = await fetch('/api/session', {method: 'POST', credentials: 'same-origin', cache: 'no-store'});
        if (!bootstrap.ok) throw new Error('A browser session could not be created. Refresh this page to retry.');
        session = await bootstrap.json();
        if (!session.csrfToken) throw new Error('A secure session could not be established. Refresh this page.');
        $('auth-status').textContent = session.authenticated ? 'Signed in · Claimed projects have no expiry.'
          : session.reauthenticationRequired ? 'Sign in again to restore account access.' : '';
        state();
        if (config.publishingEnabled) await listProjects();
      }
      status(config.publishingEnabled ? 'Choose a site to get started.' : 'Publishing is unavailable right now.', !config.publishingEnabled);
    } catch (error) { status(error.message, true); state(); }
  })();
})();

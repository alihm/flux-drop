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
  // Minimal QR encoder (byte mode, error correction M, versions 1–10), following
  // ISO/IEC 18004. Links are generated locally; nothing is sent to a QR service.
  const qrMatrix = (() => {
    const eccPerBlock = [0, 10, 16, 26, 18, 24, 16, 18, 22, 22, 26];
    const blockCount = [0, 1, 1, 1, 2, 2, 4, 4, 4, 5, 5];
    const rawModules = version => {
      let result = (16 * version + 128) * version + 64;
      if (version >= 2) {
        const align = Math.floor(version / 7) + 2;
        result -= (25 * align - 10) * align - 55;
        if (version >= 7) result -= 36;
      }
      return result;
    };
    const dataCodewords = version => Math.floor(rawModules(version) / 8) - eccPerBlock[version] * blockCount[version];
    const multiply = (x, y) => {
      let z = 0;
      for (let i = 7; i >= 0; i--) {
        z = (z << 1) ^ ((z >>> 7) * 0x11d);
        z ^= ((y >>> i) & 1) * x;
      }
      return z;
    };
    const divisor = degree => {
      const result = new Array(degree).fill(0);
      result[degree - 1] = 1;
      let root = 1;
      for (let i = 0; i < degree; i++) {
        for (let j = 0; j < degree; j++) {
          result[j] = multiply(result[j], root);
          if (j + 1 < degree) result[j] ^= result[j + 1];
        }
        root = multiply(root, 2);
      }
      return result;
    };
    const remainder = (data, generator) => {
      const result = new Array(generator.length).fill(0);
      for (const byte of data) {
        const factor = byte ^ result.shift();
        result.push(0);
        generator.forEach((coefficient, i) => { result[i] ^= multiply(coefficient, factor); });
      }
      return result;
    };
    const alignmentPositions = (version, size) => {
      if (version === 1) return [];
      const count = Math.floor(version / 7) + 2;
      const step = Math.ceil((version * 4 + 4) / (count * 2 - 2)) * 2;
      const result = [6];
      for (let position = size - 7; result.length < count; position -= step) result.splice(1, 0, position);
      return result;
    };

    return text => {
      const bytes = [...new TextEncoder().encode(text)];
      let version = 1;
      for (; version <= 10; version++) {
        if (4 + (version < 10 ? 8 : 16) + bytes.length * 8 <= dataCodewords(version) * 8) break;
      }
      if (version > 10) return null;
      const capacity = dataCodewords(version) * 8;
      const bits = [];
      const push = (value, length) => { for (let i = length - 1; i >= 0; i--) bits.push((value >>> i) & 1); };
      push(4, 4);
      push(bytes.length, version < 10 ? 8 : 16);
      for (const byte of bytes) push(byte, 8);
      push(0, Math.min(4, capacity - bits.length));
      push(0, (8 - bits.length % 8) % 8);
      for (let pad = 0xec; bits.length < capacity; pad ^= 0xec ^ 0x11) push(pad, 8);
      const data = [];
      for (let i = 0; i < bits.length; i += 8) data.push(bits.slice(i, i + 8).reduce((byte, bit) => byte << 1 | bit, 0));

      const blocks = blockCount[version], eccLength = eccPerBlock[version];
      const rawCodewords = Math.floor(rawModules(version) / 8);
      const shortBlocks = blocks - rawCodewords % blocks, shortLength = Math.floor(rawCodewords / blocks);
      const generator = divisor(eccLength);
      const split = [];
      for (let i = 0, offset = 0; i < blocks; i++) {
        const block = data.slice(offset, offset += shortLength - eccLength + (i < shortBlocks ? 0 : 1));
        const ecc = remainder(block, generator);
        if (i < shortBlocks) block.push(0);
        split.push(block.concat(ecc));
      }
      const codewords = [];
      for (let i = 0; i < split[0].length; i++) {
        split.forEach((block, j) => { if (i !== shortLength - eccLength || j >= shortBlocks) codewords.push(block[i]); });
      }

      const size = version * 4 + 17;
      const modules = Array.from({length: size}, () => new Array(size).fill(false));
      const reserved = Array.from({length: size}, () => new Array(size).fill(false));
      const set = (x, y, dark) => { modules[y][x] = dark; reserved[y][x] = true; };
      for (let i = 0; i < size; i++) { set(6, i, i % 2 === 0); set(i, 6, i % 2 === 0); }
      for (const [cx, cy] of [[3, 3], [size - 4, 3], [3, size - 4]]) {
        for (let dy = -4; dy <= 4; dy++) {
          for (let dx = -4; dx <= 4; dx++) {
            const x = cx + dx, y = cy + dy, distance = Math.max(Math.abs(dx), Math.abs(dy));
            if (x >= 0 && x < size && y >= 0 && y < size) set(x, y, distance !== 2 && distance !== 4);
          }
        }
      }
      const positions = alignmentPositions(version, size), last = positions.length - 1;
      positions.forEach((cy, i) => positions.forEach((cx, j) => {
        if ((i === 0 && j === 0) || (i === 0 && j === last) || (i === last && j === 0)) return;
        for (let dy = -2; dy <= 2; dy++) for (let dx = -2; dx <= 2; dx++) set(cx + dx, cy + dy, Math.max(Math.abs(dx), Math.abs(dy)) !== 1);
      }));
      const drawFormat = mask => {
        const value = mask; // error correction level M encodes as 00
        let rem = value;
        for (let i = 0; i < 10; i++) rem = (rem << 1) ^ ((rem >>> 9) * 0x537);
        const format = ((value << 10) | rem) ^ 0x5412;
        const bit = i => ((format >>> i) & 1) === 1;
        for (let i = 0; i <= 5; i++) set(8, i, bit(i));
        set(8, 7, bit(6)); set(8, 8, bit(7)); set(7, 8, bit(8));
        for (let i = 9; i < 15; i++) set(14 - i, 8, bit(i));
        for (let i = 0; i < 8; i++) set(size - 1 - i, 8, bit(i));
        for (let i = 8; i < 15; i++) set(8, size - 15 + i, bit(i));
        set(8, size - 8, true);
      };
      drawFormat(0);
      if (version >= 7) {
        let rem = version;
        for (let i = 0; i < 12; i++) rem = (rem << 1) ^ ((rem >>> 11) * 0x1f25);
        const info = (version << 12) | rem;
        for (let i = 0; i < 18; i++) {
          const dark = ((info >>> i) & 1) === 1, a = size - 11 + i % 3, b = Math.floor(i / 3);
          set(a, b, dark); set(b, a, dark);
        }
      }
      let index = 0;
      for (let right = size - 1; right >= 1; right -= 2) {
        if (right === 6) right = 5;
        for (let vertical = 0; vertical < size; vertical++) {
          for (let j = 0; j < 2; j++) {
            const x = right - j, upward = ((right + 1) & 2) === 0, y = upward ? size - 1 - vertical : vertical;
            if (!reserved[y][x] && index < codewords.length * 8) {
              modules[y][x] = ((codewords[index >>> 3] >>> (7 - (index & 7))) & 1) === 1;
              index++;
            }
          }
        }
      }

      const maskTest = [
        (x, y) => (x + y) % 2 === 0, (x, y) => y % 2 === 0, x => x % 3 === 0, (x, y) => (x + y) % 3 === 0,
        (x, y) => (Math.floor(x / 3) + Math.floor(y / 2)) % 2 === 0, (x, y) => x * y % 2 + x * y % 3 === 0,
        (x, y) => (x * y % 2 + x * y % 3) % 2 === 0, (x, y) => ((x + y) % 2 + x * y % 3) % 2 === 0
      ];
      const applyMask = mask => {
        for (let y = 0; y < size; y++) for (let x = 0; x < size; x++) if (!reserved[y][x] && maskTest[mask](x, y)) modules[y][x] = !modules[y][x];
      };
      const penalty = () => {
        let score = 0, dark = 0;
        const lines = [];
        for (let i = 0; i < size; i++) { lines.push(modules[i]); lines.push(modules.map(row => row[i])); }
        for (const line of lines) {
          let run = 1;
          for (let i = 1; i <= size; i++) {
            if (i < size && line[i] === line[i - 1]) { run++; continue; }
            if (run >= 5) score += run - 2;
            run = 1;
          }
          const padded = [...new Array(4).fill(false), ...line, ...new Array(4).fill(false)];
          for (let i = 0; i + 11 <= padded.length; i++) {
            const window = padded.slice(i, i + 11).map(Number).join('');
            if (window === '10111010000' || window === '00001011101') score += 40;
          }
        }
        for (let y = 0; y < size; y++) {
          for (let x = 0; x < size; x++) {
            if (modules[y][x]) dark++;
            if (x < size - 1 && y < size - 1 && modules[y][x] === modules[y][x + 1] && modules[y][x] === modules[y + 1][x] && modules[y][x] === modules[y + 1][x + 1]) score += 3;
          }
        }
        return score + Math.max(0, Math.ceil(Math.abs(dark * 20 - size * size * 10) / (size * size)) - 1) * 10;
      };
      let best = 0, bestScore = Infinity;
      for (let mask = 0; mask < 8; mask++) {
        applyMask(mask);
        drawFormat(mask);
        const score = penalty();
        if (score < bestScore) { best = mask; bestScore = score; }
        applyMask(mask);
      }
      applyMask(best);
      drawFormat(best);
      return modules;
    };
  })();

  const dialog = $('manage-dialog');
  let config, session, files = [], selectionError = '', key = '', cursor = '';
  let uploading = false, authenticating = false, managing = false, published = false, selecting = false, nameEdited = false;
  let currentUpload, lastPublishedProject, modalTrigger, toastTimer;

  // Theme: follows the system unless the visitor picks one. Storage is optional.
  const root = document.documentElement;
  const systemLight = window.matchMedia('(prefers-color-scheme: light)');
  try {
    const stored = localStorage.getItem('drop-theme');
    if (stored === 'light' || stored === 'dark') root.dataset.theme = stored;
  } catch { /* Storage can be unavailable in private windows. */ }
  const effectiveTheme = () => root.dataset.theme || (systemLight.matches ? 'light' : 'dark');
  function syncThemeButton() {
    const label = `Switch to ${effectiveTheme() === 'dark' ? 'light' : 'dark'} theme`;
    $('theme-toggle').setAttribute('aria-label', label);
    $('theme-toggle').title = label;
  }
  $('theme-toggle').onclick = () => {
    root.dataset.theme = effectiveTheme() === 'dark' ? 'light' : 'dark';
    try { localStorage.setItem('drop-theme', root.dataset.theme); } catch { /* Keep the choice for this page only. */ }
    syncThemeButton();
  };
  systemLight.addEventListener?.('change', syncThemeButton);
  syncThemeButton();

  $('name').value = randomName();
  $('year').textContent = String(new Date().getFullYear());
  $('example-url').textContent = `${window.location.host}/${$('name').value}-3f9a2c/`;

  function status(message, isError = false) {
    $('status').textContent = message;
    $('status').classList.toggle('error', isError);
  }

  function toast(message) {
    $('toast').textContent = message;
    $('toast').hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { $('toast').hidden = true; }, 2400);
  }

  function renderUrlPreview() {
    const name = document.createElement('b');
    name.textContent = $('name').value || 'your-name';
    $('url-preview').replaceChildren(`${window.location.host}/`, name, '-xxxxxx/');
  }

  function setAuthState() {
    $('sign-in-label').textContent = 'Sign in';
    $('sign-in').hidden = !config?.firebase || !window.DropAuth || Boolean(session?.authenticated);
    $('sign-out').hidden = !session?.authenticated && !session?.reauthenticationRequired;
    $('sign-in').disabled = authenticating || !session;
    $('sign-out').disabled = authenticating || !session;
  }

  function state() {
    const nameValid = namePattern.test($('name').value);
    $('name').setAttribute('aria-invalid', String(!nameValid));
    $('name-help').textContent = nameValid
      ? 'Lowercase letters, numbers, and hyphens. A short unique suffix is added.'
      : 'Use 1–48 lowercase letters, numbers, or internal hyphens.';
    renderUrlPreview();
    $('selection-stage').hidden = !files.length || published;
    $('publish-panel').classList.toggle('is-published', published);
    $('publish-panel').classList.toggle('has-selection', files.length > 0 && !published);
    $('drop-title').textContent = files.length && !published ? 'Change your files' : 'Drop an HTML file, ZIP, or folder';
    $('claim-result').textContent = session?.authenticated ? 'Claim site' : 'Claim with Google';
    $('choose-files').disabled = uploading || selecting;
    $('choose-folder').disabled = uploading || selecting;
    $('name').disabled = uploading;
    const isPrivate = $('private-toggle').checked, passwordValid = !isPrivate || passwordProblem() === '';
    $('private-fields').hidden = !isPrivate;
    $('private-toggle').disabled = uploading;
    $('site-password').disabled = uploading;
    $('site-password').setAttribute('aria-invalid', String(isPrivate && $('site-password').value !== '' && !passwordValid));
    $('password-help').textContent = isPrivate && $('site-password').value !== '' && !passwordValid ? passwordProblem() : 'At least 12 characters. Share it only with people who should see the site.';
    $('private-warning').hidden = !isPrivate || (files.length === 1 && /\.html?$/i.test(pathOf(files[0])));
    $('publish').firstChild.textContent = isPrivate ? 'Publish private site' : 'Publish site';
    $('publish').disabled = uploading || selecting || authenticating || published || !config?.publishingEnabled || !session || !files.length || Boolean(selectionError) || !nameValid || !passwordValid;
    $('clear-selection').disabled = uploading || selecting;
    setAuthState();
  }

  function passwordProblem() {
    const value = $('site-password').value;
    if ([...value].length < 12) return 'Use at least 12 characters.';
    if (new TextEncoder().encode(value).length > 1024) return 'Use at most 1,024 bytes.';
    return '';
  }
  // Header values must be ASCII, so the password travels as base64url-encoded UTF-8.
  const encodePassword = value => btoa(String.fromCharCode(...new TextEncoder().encode(value))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

  const fileOf = item => item.file || item;
  const pathOf = item => item.path || item.webkitRelativePath || item.name;

  function selectionProblem(selected) {
    if (!config) return 'The service is still connecting. Try again in a moment.';
    if (selected.length > config.limits.files) return `Choose no more than ${config.limits.files.toLocaleString()} files.`;
    if (selected.reduce((sum, item) => sum + fileOf(item).size, 0) > config.limits.uploadBytes) return 'This selection exceeds the upload limit. Choose a smaller site.';
    if (!selected.length) return '';
    if (selected.length === 1 && /\.(html?|zip)$/i.test(pathOf(selected[0]))) return '';
    const paths = selected.map(pathOf);
    if (paths.includes('index.html')) return '';
    const roots = new Set(paths.map(path => path.split('/')[0]));
    if (roots.size === 1 && paths.includes([...roots][0] + '/index.html')) return '';
    return 'Include an index.html at the site root, or choose one HTML or ZIP file.';
  }

  function renderSelection() {
    const total = files.reduce((sum, item) => sum + fileOf(item).size, 0);
    $('selection').textContent = files.length
      ? `${files.length} file${files.length === 1 ? '' : 's'} · ${bytesLabel(total)}`
      : 'No files selected';
    $('clear-selection').hidden = !files.length;
    const list = $('selected-files');
    list.replaceChildren();
    for (const file of files.slice(0, 6)) {
      const item = document.createElement('li');
      const name = document.createElement('span'); name.textContent = pathOf(file);
      const size = document.createElement('span'); size.className = 'size'; size.textContent = bytesLabel(fileOf(file).size);
      item.append(name, size);
      list.append(item);
    }
    if (files.length > 6) {
      const item = document.createElement('li');
      item.className = 'more';
      item.textContent = `…and ${(files.length - 6).toLocaleString()} more`;
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

  // Suggest a project name from the dropped folder or file, e.g. "My Portfolio/" -> "my-portfolio".
  function suggestedName(selected) {
    if (!selected.length) return '';
    const first = pathOf(selected[0]);
    if (selected.length > 1 && !first.includes('/')) return '';
    const base = (selected.length === 1 ? first.split('/').pop().replace(/\.(html?|zip)$/i, '') : first.split('/')[0])
      .normalize('NFKD').replace(/[̀-ͯ]/g, '').toLowerCase()
      .replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 48).replace(/-+$/, '');
    return ['index', 'dist', 'build', 'public', 'out', 'site', 'www'].includes(base) || !namePattern.test(base) ? '' : base;
  }

  function choose(selected) {
    if (uploading) return;
    files = selected.map(item => item.file ? item : {file: item, path: pathOf(item)});
    if (!nameEdited) $('name').value = suggestedName(files) || $('name').value;
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
  $('name').oninput = () => { nameEdited = true; key = crypto.randomUUID(); state(); };
  // A new retry key keeps a public attempt and a private attempt from being merged.
  $('private-toggle').onchange = () => {
    key = crypto.randomUUID();
    state();
    if ($('private-toggle').checked) $('site-password').focus();
  };
  $('site-password').oninput = () => state();
  $('site-password').addEventListener('keydown', event => {
    if (event.key === 'Enter' && !event.isComposing) { event.preventDefault(); $('publish').click(); }
  });
  $('toggle-password').onclick = () => {
    const show = $('site-password').type === 'password';
    $('site-password').type = show ? 'text' : 'password';
    $('toggle-password').textContent = show ? 'Hide' : 'Show';
    $('toggle-password').setAttribute('aria-pressed', String(show));
  };
  $('name').addEventListener('keydown', event => {
    if (event.key === 'Enter' && !event.isComposing) { event.preventDefault(); $('publish').click(); }
  });

  const zone = $('dropzone');
  // A click on the empty drop area opens the file picker, like the buttons do.
  zone.addEventListener('click', event => {
    if (event.target.closest('button') || files.length || uploading || selecting) return;
    $('files').click();
  });
  window.addEventListener('beforeunload', event => {
    if (uploading) { event.preventDefault(); event.returnValue = ''; }
  });
  async function droppedFiles(items, fallback) {
    const sources = items.filter(item => item.kind === 'file').map(item => ({entry: item.webkitGetAsEntry?.(), file: item.getAsFile?.()}));
    if (!sources.length) return fallback.map(file => ({file, path: file.name}));
    const selected = [];
    let bytes = 0;
    const add = (file, path) => {
      selected.push({file, path});
      bytes += file.size;
      if (config && selected.length > config.limits.files) throw new Error(`Choose no more than ${config.limits.files.toLocaleString()} files.`);
      if (config && bytes > config.limits.uploadBytes) throw new Error('This selection exceeds the upload limit. Choose a smaller site.');
    };
    const walk = async (entry, prefix = '') => {
      const path = prefix + entry.name;
      if (entry.isFile) {
        const file = await new Promise((resolve, reject) => entry.file(resolve, reject));
        add(file, path);
      } else if (entry.isDirectory) {
        const reader = entry.createReader();
        while (true) {
          const batch = await new Promise((resolve, reject) => reader.readEntries(resolve, reject));
          if (!batch.length) break;
          for (const child of batch) await walk(child, path + '/');
        }
      }
    };
    for (const source of sources) {
      if (source.entry) await walk(source.entry);
      else if (source.file) add(source.file, source.file.name);
      else throw new Error('This browser cannot read a dropped folder. Use Choose folder instead.');
    }
    if (!selected.length) throw new Error('No files were found. Choose a folder with an index.html file.');
    return selected;
  }
  // Files can be dropped anywhere on the page; the overlay shows where they go.
  let dragDepth = 0;
  const draggingFiles = event => [...(event.dataTransfer?.types || [])].includes('Files');
  const canDrop = () => !uploading && !selecting && !dialog.open;
  const hideOverlay = () => { dragDepth = 0; $('drop-overlay').hidden = true; zone.classList.remove('drag'); };
  document.addEventListener('dragenter', event => {
    if (!draggingFiles(event) || !canDrop()) return;
    event.preventDefault();
    dragDepth++;
    $('drop-overlay').hidden = false;
    zone.classList.add('drag');
  });
  document.addEventListener('dragover', event => {
    if (!draggingFiles(event)) return;
    event.preventDefault();
    event.dataTransfer.dropEffect = canDrop() ? 'copy' : 'none';
  });
  document.addEventListener('dragleave', event => {
    if (!draggingFiles(event)) return;
    dragDepth = Math.max(0, dragDepth - 1);
    if (!dragDepth) hideOverlay();
  });
  window.addEventListener('blur', hideOverlay);
  document.addEventListener('drop', async event => {
    if (event.dataTransfer?.types && !draggingFiles(event)) return;
    event.preventDefault();
    hideOverlay();
    if (!canDrop()) return;
    const items = [...(event.dataTransfer?.items || [])];
    const fallback = [...(event.dataTransfer?.files || [])];
    selecting = true;
    state();
    status('Reading your files…');
    $('publish-panel').scrollIntoView({behavior: 'smooth', block: 'start'});
    try { choose(await droppedFiles(items, fallback)); }
    catch (error) { status(error.message || 'Could not read those files. Try Choose folder.', true); }
    finally { selecting = false; state(); }
  });

  function upload(url, body, headers, onProgress) {
    return new Promise((resolve, reject) => {
      const request = new XMLHttpRequest();
      currentUpload = request;
      request.open('POST', url);
      for (const [name, value] of Object.entries(headers)) request.setRequestHeader(name, value);
      request.upload.onprogress = event => {
        if (event.lengthComputable) onProgress(Math.min(100, Math.round(event.loaded * 100 / event.total)), event.loaded, event.total);
      };
      request.onload = () => {
        currentUpload = null;
        let data;
        try { data = JSON.parse(request.responseText); }
        catch { reject(new Error('The server returned an unexpected response. Check your sites before retrying.')); return; }
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
    nameEdited = false;
    $('name').value = randomName();
    $('result').hidden = true;
    status('Choose a site to get started.');
    state();
    zone.scrollIntoView({behavior: 'smooth', block: 'center'});
  };

  async function copyLink(url, button, message) {
    try {
      await navigator.clipboard.writeText(url);
      if (button.dataset.label === undefined) button.dataset.label = button.textContent;
      button.textContent = 'Copied ✓';
      clearTimeout(Number(button.dataset.timer));
      button.dataset.timer = String(setTimeout(() => { button.textContent = button.dataset.label; }, 2000));
      toast('Link copied to clipboard');
    } catch { message.textContent = 'Copy was blocked by your browser. Open the site and copy its address.'; }
  }
  $('copy-result').onclick = () => copyLink($('project-link').href, $('copy-result'), $('status'));

  function renderQR(container, url) {
    const matrix = qrMatrix(url);
    container.replaceChildren();
    if (!matrix) return false;
    const ns = 'http://www.w3.org/2000/svg';
    const svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('viewBox', `-2 -2 ${matrix.length + 4} ${matrix.length + 4}`);
    svg.setAttribute('shape-rendering', 'crispEdges');
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', 'QR code for ' + url);
    const path = document.createElementNS(ns, 'path');
    let d = '';
    matrix.forEach((row, y) => row.forEach((dark, x) => { if (dark) d += `M${x} ${y}h1v1h-1z`; }));
    path.setAttribute('d', d);
    path.setAttribute('fill', '#0b121c');
    svg.append(path);
    container.append(svg);
    return true;
  }

  function downloadQR(url, name) {
    const matrix = qrMatrix(url);
    if (!matrix) return;
    const scale = 12, quiet = 4, size = (matrix.length + quiet * 2) * scale;
    const canvas = document.createElement('canvas');
    canvas.width = canvas.height = size;
    const context = canvas.getContext('2d');
    context.fillStyle = '#fff';
    context.fillRect(0, 0, size, size);
    context.fillStyle = '#000';
    matrix.forEach((row, y) => row.forEach((dark, x) => { if (dark) context.fillRect((x + quiet) * scale, (y + quiet) * scale, scale, scale); }));
    canvas.toBlob(blob => {
      if (!blob) return;
      const link = document.createElement('a');
      link.href = URL.createObjectURL(blob);
      link.download = `${name}-qr.png`;
      link.click();
      setTimeout(() => URL.revokeObjectURL(link.href), 1000);
    });
  }

  const dayMs = 86400000;
  const relative = new Intl.RelativeTimeFormat(undefined, {numeric: 'auto'});
  function daysUntil(value) {
    const time = new Date(value).getTime();
    return Number.isNaN(time) ? NaN : Math.ceil((time - Date.now()) / dayMs);
  }
  function expiryLabel(value) {
    const days = daysUntil(value);
    if (Number.isNaN(days)) return '';
    return days <= 0 ? 'Expires today' : `Expires ${relative.format(days, 'day')}`;
  }
  function ageLabel(value) {
    const time = new Date(value).getTime();
    if (Number.isNaN(time)) return '';
    return relative.format(-Math.floor((Date.now() - time) / dayMs), 'day');
  }

  function publishError(code, data) {
    if (code === 413) return 'The upload is too large, including ZIP or form packaging.';
    if (code === 429) return 'Too many requests. Wait a moment and retry.';
    if (code === 409) return data.error === 'name_conflict' ? 'That name is in use. Choose another.' : 'The name, quota, or site state conflicts. Check your sites before retrying.';
    if (code === 429 && data.error === 'password_busy') return 'The service is busy protecting another site. Retry in a moment.';
    if (code === 400 && data.error === 'invalid_password') return 'Use a password with at least 12 characters.';
    if (code === 400) return 'Check your files and site name, then retry.';
    return 'Publishing could not be confirmed. Retry this selection safely, or check your sites.';
  }

  $('publish').onclick = async () => {
    if ($('publish').disabled) return;
    uploading = true;
    state();
    $('upload-progress').hidden = false;
    $('progress-fill').style.width = '0%';
    $('upload-progress').querySelector('[role="progressbar"]').setAttribute('aria-valuenow', '0');
    $('progress-label').textContent = 'Uploading…';
    status('Uploading and verifying your files…');
    try {
      const body = new FormData();
      for (const item of files) body.append('files', fileOf(item), pathOf(item));
      const response = await upload('/api/projects?name=' + encodeURIComponent($('name').value), body,
        {'X-CSRF-Token': session.csrfToken, 'Idempotency-Key': key, ...($('private-toggle').checked ? {'X-Drop-Password': encodePassword($('site-password').value)} : {})}, (percent, loaded, total) => {
          $('progress-fill').style.width = percent + '%';
          $('upload-progress').querySelector('[role="progressbar"]').setAttribute('aria-valuenow', String(percent));
          $('progress-label').textContent = percent === 100 ? 'Upload complete. Publishing…' : `${bytesLabel(loaded)} of ${bytesLabel(total)} · ${percent}%`;
        });
      const {status: code, data} = response;
      if (code < 200 || code >= 300) {
        if (!(code === 409 && data.error === 'duplicate_content')) throw new Error(publishError(code, data));
      }
      if (typeof data.path !== 'string' || !pathPattern.test(data.path)) throw new Error('The server returned an invalid site link.');
      published = true;
      lastPublishedProject = code < 300 && data.project?.expiresAt && /^[a-f0-9]{32}$/.test(data.project?.id) ? data.project : null;
      $('result-kicker').textContent = code < 300 ? 'Published on Flux' : 'Already live';
      $('result-label').textContent = code < 300 ? 'Your site is live.' : 'These files are already published.';
      const publishedPrivate = code < 300 && data.project?.private === true;
      if (code < 300) $('result-kicker').textContent = publishedPrivate ? 'Published privately' : 'Published on Flux';
      $('result-subtitle').textContent = code >= 300 ? 'Use the existing link below; no duplicate was created and its access settings were not changed.'
        : publishedPrivate ? 'Only people with the password can open it.' : 'Your link is ready to share.';
      $('site-password').value = '';
      const siteURL = new URL(data.path, window.location.origin).href;
      $('project-link').href = data.path;
      $('project-link').textContent = window.location.host + data.path;
      $('open-result').href = data.path;
      $('result').querySelector('.qr-card').hidden = !renderQR($('result-qr'), siteURL);
      $('download-qr').onclick = () => downloadQR(siteURL, data.path.slice(1, -1));
      $('share-result').hidden = typeof navigator.share !== 'function';
      $('share-result').onclick = () => navigator.share({title: 'My site on Flux Drop', url: siteURL}).catch(() => {});
      $('claim-result').hidden = !lastPublishedProject;
      $('claim-callout').hidden = !lastPublishedProject;
      $('claim-title').textContent = lastPublishedProject ? expiryLabel(lastPublishedProject.expiresAt) || 'Expires in 30 days' : '';
      $('expiry').textContent = lastPublishedProject
        ? `Unclaimed sites are kept until ${dateLabel(lastPublishedProject.expiresAt)}. Claim it with Google to keep it with no expiry and manage it from any device.`
        : '';
      $('result').hidden = false;
      // Collapse the upload area before scrolling so the result lands where expected.
      $('upload-progress').hidden = true;
      state();
      status(code < 300 ? 'Published successfully.' : 'No duplicate site was created.');
      if (code < 300) toast('Your site is live');
      $('result-label').focus({preventScroll: true});
      $('result').scrollIntoView({behavior: 'smooth', block: 'start'});
      await listProjects();
    } catch (error) {
      status(error.name === 'AbortError'
        ? 'Upload stopped. It may still complete on the server; retry the same selection or check your sites.'
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
    $('auth-status').textContent = window.DropAuth.preview ? 'Signing in to the local UI demo…' : 'Complete sign-in in the Google window.';
    try {
      // Open the popup in the click handler's user gesture, before any network await.
      const idToken = await window.DropAuth.token();
      await exchange('/api/auth/google', {idToken});
      $('auth-status').textContent = 'Signed in. Claimed sites stay available across devices.';
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
    if (!window.confirm('Sign out? Unclaimed sites in this browser will no longer be available. Claim them first to keep access.')) return;
    authenticating = true;
    state();
    try {
      await exchange('/api/auth/logout', {});
      try { await window.DropAuth?.clear(); } catch { /* The server session is already signed out. */ }
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
    message.textContent = 'Claiming site…';
    try {
      const response = await fetch('/api/projects/' + project.id + '/claim', {
        method: 'POST', headers: {'Content-Type': 'application/json', 'X-CSRF-Token': session.csrfToken, 'If-Match': `"${project.revision}"`}, body: '{}'
      });
      if (!response.ok) throw new Error(response.status === 409 || response.status === 404
        ? 'This site changed. Refresh your sites, then claim it.'
        : 'Could not confirm the claim. Refresh your sites before retrying.');
      if (lastPublishedProject?.id === project.id) {
        lastPublishedProject = null;
        $('claim-result').hidden = true;
        $('claim-callout').hidden = true;
        $('expiry').textContent = 'Claimed. This site no longer expires.';
        $('result-subtitle').textContent = 'Claimed. Your site has no expiry.';
      }
      message.textContent = 'Claimed. This site no longer expires.';
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
    const expiry = document.createElement('span');
    if (project.expiresAt) {
      expiry.textContent = expiryLabel(project.expiresAt) || `Expires ${dateLabel(project.expiresAt)}`;
      expiry.title = dateLabel(project.expiresAt);
      if (daysUntil(project.expiresAt) <= 7) expiry.className = 'expiring';
    } else {
      expiry.textContent = 'Claimed · No expiry';
      expiry.className = 'kept';
    }
    meta.append(expiry);
    if (project.createdAt && ageLabel(project.createdAt)) {
      const created = document.createElement('span');
      created.textContent = `Created ${ageLabel(project.createdAt)}`;
      created.title = dateLabel(project.createdAt);
      meta.append(created);
    }
    if (bytesLabel(project.bytes)) {
      const size = document.createElement('span'); size.textContent = bytesLabel(project.bytes); meta.append(size);
    }
    card.append(meta);
    const actions = document.createElement('div'); actions.className = 'card-actions';
    const open = document.createElement('a'); open.href = path; open.target = '_blank'; open.rel = 'noopener noreferrer'; open.textContent = 'Open site ↗'; open.setAttribute('aria-label', `Open ${project.slug} in a new tab`); actions.append(open);
    const message = document.createElement('p'); message.className = 'card-status'; message.setAttribute('role', 'status');
    addButton(actions, 'Copy link', 'secondary', event => copyLink(new URL(path, window.location.origin).href, event.currentTarget, message));
    if (project.expiresAt) addButton(actions, 'Keep this site', 'claim', () => claimProject(project, message));
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
      if (!response.ok) throw new Error('Could not load your sites. Refresh to try again.');
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
        ? `${count}${cursor ? '+' : ''} site${count === 1 ? '' : 's'} ${session.authenticated ? 'in your account and this browser' : 'in this browser'}.`
        : session.authenticated ? 'No sites yet. Publish your first one above.' : '';
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
    for (const [name, title] of [['content', 'Content'], ['access', 'Access'], ['share', 'Share'], ['settings', 'Settings']]) {
      const panel = document.createElement('section'); panel.className = 'manage-panel'; panel.hidden = name !== 'content';
      const tab = addButton(tabs, title, '', () => {
        for (const item of tabItems) { item.panel.hidden = item.name !== name; item.tab.setAttribute('aria-pressed', String(item.name === name)); }
      });
      tab.setAttribute('aria-pressed', String(name === 'content'));
      tabItems.push({name, tab, panel}); panels.append(panel);
    }
    content.append(tabs, panels);
    const [contentPanel, accessPanel, sharePanel, settingsPanel] = tabItems.map(item => item.panel);
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
          if (response.status === 409 || response.status === 404) { stale = true; throw new Error('This site changed. Close settings and refresh your sites.'); }
          throw new Error(response.status === 400 ? 'Check your input and try again.' : 'The result could not be confirmed. Refresh your sites before retrying.');
        }
        dialog.close();
        await listProjects();
        status('Site updated.');
      } catch (error) {
        setMessage(error instanceof TypeError ? 'Connection interrupted. Refresh your sites to check whether the change completed.' : error.message, true);
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
      if (!window.confirm('Replace this site’s current content? Its URL will stay the same.')) return;
      const body = new FormData();
      for (const file of replacement) body.append('files', file, file.webkitRelativePath || file.name);
      perform('POST', '/versions', body, {'Idempotency-Key': replacementKey});
    });

    const accessTitle = document.createElement('h3'); accessTitle.textContent = project.private ? 'Private access' : 'Public access'; accessPanel.append(accessTitle);
    note(accessPanel, project.private ? 'Visitors need the password to open this site.' : 'Anyone with this URL can view the site.');
    const password = field(accessPanel, project.private ? 'New site password' : 'Password for private access', 'password');
    password.autocomplete = 'new-password'; password.maxLength = 1024; passwordInputs.push(password);
    note(accessPanel, 'Use at least 12 characters. Private sites currently support only self-contained index.html pages, without separate assets.');
    addButton(accessPanel, project.private ? 'Change password' : 'Make private', 'secondary', () => {
      if ([...password.value].length < 12 || new TextEncoder().encode(password.value).length > 1024) { setMessage('Use at least 12 characters and at most 1,024 UTF-8 bytes.', true); return; }
      perform('PUT', '/privacy', {private: true, password: password.value});
    });
    if (project.private) addButton(accessPanel, 'Make public', 'secondary', () => {
      if (window.confirm('Make this site public? Anyone with its link will be able to view it.')) perform('PUT', '/privacy', {private: false});
    });

    const siteURL = new URL('/' + project.slug + '/', window.location.origin).href;
    const share = document.createElement('div'); share.className = 'share-panel';
    const code = document.createElement('div'); code.className = 'qr';
    const shareText = document.createElement('div');
    const shareTitle = document.createElement('h3'); shareTitle.textContent = 'Share this site'; shareText.append(shareTitle);
    note(shareText, project.private ? 'Visitors will be asked for the site password.' : 'Anyone with the link or QR code can open this site.');
    const copyButton = addButton(shareText, 'Copy link', 'secondary', () => copyLink(siteURL, copyButton, message));
    addButton(shareText, 'Download QR code', 'secondary', () => downloadQR(siteURL, project.slug));
    share.append(code, shareText);
    sharePanel.append(share);
    if (!renderQR(code, siteURL)) code.hidden = true;

    const settingsTitle = document.createElement('h3'); settingsTitle.textContent = 'Site name'; settingsPanel.append(settingsTitle);
    note(settingsPanel, 'Renaming keeps the six-character suffix. Existing links may continue to redirect.');
    const rename = field(settingsPanel, 'New site name', 'text', project.slug.slice(0, -7)); rename.maxLength = 48;
    addButton(settingsPanel, 'Rename', 'secondary', () => {
      if (!namePattern.test(rename.value)) { setMessage('Use 1–48 lowercase letters, numbers, or internal hyphens.', true); return; }
      perform('PATCH', '', {name: rename.value});
    });
    const dangerZone = document.createElement('div'); dangerZone.className = 'danger-zone'; settingsPanel.append(dangerZone);
    const deleteTitle = document.createElement('h3'); deleteTitle.textContent = 'Delete site'; dangerZone.append(deleteTitle);
    note(dangerZone, `Deleting removes the public URL and site access. This cannot be undone. Type ${project.slug} to confirm.`);
    const deletion = field(dangerZone, 'Type the full site URL name to confirm', 'text'); deletion.autocomplete = 'off'; deletion.spellcheck = false;
    addButton(dangerZone, 'Delete site', 'danger', () => {
      if (deletion.value !== project.slug) { setMessage('Enter the exact site URL name to confirm deletion.', true); return; }
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
      const megabytes = `${Math.floor(config.limits.uploadBytes / 1048576)} MiB`, fileCount = config.limits.files.toLocaleString();
      $('limits').textContent = `Up to ${megabytes} · ${fileCount} files`;
      for (const item of document.querySelectorAll('.limit-bytes')) item.textContent = megabytes;
      for (const item of document.querySelectorAll('.limit-files')) item.textContent = fileCount;
      state();
      if (config.authenticationEnabled) {
        const bootstrap = await fetch('/api/session', {method: 'POST', credentials: 'same-origin', cache: 'no-store'});
        // A revoked cookie is HttpOnly, so the page cannot clear it; tell the visitor how.
        if (bootstrap.status === 401) throw new Error('Your saved session is no longer valid. Clear this site’s cookies, then reload the page.');
        if (bootstrap.status === 429) throw new Error('Too many new sessions from this network. Wait a minute, then reload the page.');
        if (!bootstrap.ok) throw new Error('A browser session could not be created. Refresh this page to retry.');
        session = await bootstrap.json();
        if (!session.csrfToken) throw new Error('A secure session could not be established. Refresh this page.');
        $('auth-status').textContent = session.authenticated ? 'Signed in. Claimed sites never expire.'
          : session.reauthenticationRequired ? 'Sign in again to restore account access.'
            : window.DropAuth?.preview ? 'UI preview · Google sign-in is simulated.' : '';
        state();
        if (config.publishingEnabled) await listProjects();
      }
      status(config.publishingEnabled ? 'Choose a site to get started.' : 'Publishing is unavailable right now.', !config.publishingEnabled);
    } catch (error) { status(error.message, true); state(); }
  })();
})();

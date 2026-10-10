"use strict";
(() => {
  const $ = id => document.getElementById(id);
  const pathPattern = /^\/[a-z0-9](?:[a-z0-9-]{0,111}[a-z0-9])?\/$/;
  const projectName = project => project.initialSuffix && project.slug.endsWith("-" + project.initialSuffix) ? project.slug.slice(0, -project.initialSuffix.length - 1) : project.slug;
  function projectPath(project) {
    if (!project || typeof project.slug !== 'string') return null;
    const path = '/' + project.slug + '/';
    return pathPattern.test(path) ? path : null;
  }
  const relative = new Intl.RelativeTimeFormat(undefined, {numeric: 'auto'});
  function ageLabel(value) {
    const time = new Date(value).getTime();
    return Number.isNaN(time) ? '' : relative.format(-Math.floor((Date.now() - time) / 86400000), 'day');
  }
  let config;
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

  const thumbnailURLs = new Set();
  const thumbnailObserver = 'IntersectionObserver' in window ? new IntersectionObserver(entries => {
    for (const entry of entries) if (entry.isIntersecting) { thumbnailObserver.unobserve(entry.target); entry.target.loadPreview?.(); }
  }, {rootMargin: '120px'}) : null;
  function thumbnail(project) {
    const frame = document.createElement('div'); frame.className = 'site-thumbnail pending';
    const label = document.createElement('span'); label.className = 'thumbnail-label'; label.textContent = 'Preview preparing'; frame.append(label);
    const version = project.digest || project.thumbnail?.split('?v=')[1];
    if (!/^[a-f0-9]{32}$/.test(project.id) || !/^[a-f0-9]{64}$/.test(version || '')) { label.textContent = 'Site preview'; return frame; }
    const src = '/api/projects/' + project.id + '/thumbnail?v=' + version;
    let attempts = 0;
    frame.loadPreview = async () => {
      if (!frame.isConnected) return;
      try {
        const response = await fetch(src, {cache: 'default', credentials: 'omit', signal: AbortSignal.timeout(12000)});
        if (!frame.isConnected) return;
        if (!response.ok) { label.textContent = 'Preview unavailable'; frame.classList.remove('pending'); return; }
        if (response.headers.get('X-Drop-Preview') !== 'ready') {
          if (++attempts <= 6) window.setTimeout(frame.loadPreview, Math.min(30000, 2000 * 2 ** attempts));
          return;
        }
        if (!response.headers.get('Content-Type')?.startsWith('image/jpeg')) throw new Error('Invalid thumbnail');
        const url = URL.createObjectURL(await response.blob()); thumbnailURLs.add(url);
        const img = document.createElement('img'); img.src = url; img.alt = 'Preview of ' + project.slug; img.decoding = 'async'; img.width = 320; img.height = 180;
        img.onload = () => { URL.revokeObjectURL(url); thumbnailURLs.delete(url); };
        img.onerror = () => { URL.revokeObjectURL(url); thumbnailURLs.delete(url); frame.replaceChildren(label); label.textContent = 'Preview unavailable'; };
        frame.classList.remove('pending'); frame.replaceChildren(img);
      } catch { if (frame.isConnected) { label.textContent = 'Preview unavailable'; frame.classList.remove('pending'); } }
    };
    if (thumbnailObserver) thumbnailObserver.observe(frame);
    else window.setTimeout(frame.loadPreview, 0);
    return frame;
  }
  window.addEventListener('pagehide', () => { for (const url of thumbnailURLs) URL.revokeObjectURL(url); thumbnailURLs.clear(); });
  let exploring = false;
  async function loadExplore() {
    if (!config?.exploreEnabled || exploring) return;
    exploring = true; $('refresh-explore').disabled = true;
    $('explore').hidden = false; $('explore-nav').hidden = false;
    $('explore-status').textContent = 'Finding the latest deployments…';
    try {
      const response = await fetch('/api/explore', {cache: 'default', credentials: 'omit', signal: AbortSignal.timeout(10000)});
      if (!response.ok) throw new Error('Could not load deployments. Try refreshing.');
      const data = await response.json(); const cards = [];
      for (const project of (data.projects || []).slice(0, 24)) {
        const path = projectPath(project); if (!path || !/^[a-f0-9]{32}$/.test(project.id) || project.private || project.claimed !== true) continue;
        const card = document.createElement('a'); card.className = 'explore-card'; card.href = path; card.target = '_blank'; card.rel = 'noopener noreferrer';
        card.setAttribute('aria-label', 'Open ' + project.slug + ' in a new tab'); card.append(thumbnail(project));
        const info = document.createElement('div'); info.className = 'explore-card-info';
        const title = document.createElement('h3'); title.textContent = projectName(project).replace(/-/g, ' ');
        const arrow = document.createElement('span'); arrow.className = 'explore-arrow'; arrow.textContent = '↗'; arrow.setAttribute('aria-hidden', 'true');
        const meta = document.createElement('p'); meta.textContent = 'Deployed ' + (ageLabel(project.updatedAt) || 'recently');
        info.append(title, arrow, meta); card.append(info); cards.push(card);
      }
      for (const frame of $('explore-list').querySelectorAll('.site-thumbnail')) thumbnailObserver?.unobserve(frame);
      $('explore-list').replaceChildren(...cards);
      $('explore-status').textContent = cards.length ? '' : 'The next great idea could be yours. Publish a public site to get things started.';
    } catch (error) { $('explore-status').textContent = error.message; }
    finally { exploring = false; $('refresh-explore').disabled = false; }
  }
  $('refresh-explore').onclick = () => loadExplore();

  $('year').textContent = String(new Date().getFullYear());
  // Preserve existing one-use claim links when handing visitors to the workspace.
  const claim = new URLSearchParams(window.location.hash.slice(1)).get('claim-token');
  if (claim && /^[a-f0-9]{32}\.[A-Za-z0-9_-]{43}$/.test(claim)) {
    const target = new URL('https://runonflux.com/apps/drop');
    target.hash = new URLSearchParams({'claim-token': claim}).toString();
    for (const link of document.querySelectorAll('.landing-link')) link.href = target.href;
    $('start-publishing').firstChild.textContent = 'Open Drop to claim this site ';
    history.replaceState(null, '', window.location.pathname + window.location.search);
  }
  (async () => {
    try {
      const response = await fetch('/api/config', {cache: 'default', credentials: 'omit', signal: AbortSignal.timeout(10000)});
      if (!response.ok) return;
      config = await response.json();
      if (config.limits) {
        for (const item of document.querySelectorAll('.limit-bytes')) item.textContent = Math.floor(config.limits.uploadBytes / 1048576) + ' MiB';
        for (const item of document.querySelectorAll('.limit-files')) item.textContent = Number(config.limits.files).toLocaleString();
      }
      if (config.exploreEnabled) void loadExplore();
    } catch { /* The workspace links and marketing page work without the API. */ }
  })();
})();

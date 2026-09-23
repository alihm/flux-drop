import { test, expect } from '@playwright/test';

const origin = 'https://localhost:18443';

async function management(page, unlock = false) {
  await page.goto('/manage');
  const csrf = await page.locator('meta[name="csrf"]').getAttribute('content');
  if (unlock) {
    const response = await page.request.post('/api/unlock', {
      headers: { Origin: origin, 'X-CSRF-Token': csrf },
    });
    expect(response.status()).toBe(204);
  }
  return csrf;
}

test('legitimate management mutation succeeds, missing CSRF fails', async ({ page }) => {
  const csrf = await management(page);
  const denied = await page.request.post('/api/mutate', { headers: { Origin: origin } });
  expect(denied.status()).toBe(403);
  const allowed = await page.request.post('/api/mutate', {
    headers: { Origin: origin, 'X-CSRF-Token': csrf },
  });
  expect(allowed.status()).toBe(204);
  const state = await page.request.get('/api/state');
  expect((await state.json()).mutations).toBe(1);
});

test('opaque uploaded HTML runs scripts but cannot read cookies or storage', async ({ page }) => {
  await management(page);
  const response = await page.goto('/public-123456/');
  expect(response.headers()['content-security-policy']).toContain('sandbox allow-scripts;');
  expect(response.headers()['content-security-policy']).not.toContain('allow-same-origin');
  const result = await page.evaluate(() => {
    const attempt = fn => { try { fn(); return 'allowed'; } catch { return 'blocked'; } };
    return {
      script: window.inlineScriptRan,
      cookie: attempt(() => document.cookie),
      storage: attempt(() => localStorage.getItem('token')),
      indexedDB: attempt(() => indexedDB.open('management')),
    };
  });
  expect(result).toEqual({ script: true, cookie: 'blocked', storage: 'blocked', indexedDB: 'blocked' });
});

test('obfuscated script cannot fetch management secrets or perform mutations', async ({ page }) => {
  // Deliberately give the attack a real CSRF value: the origin boundary must
  // still stop it. This also ensures failure is not simply an absent session.
  const csrf = await management(page);
  await page.goto('/public-123456/');
  const result = await page.evaluate(async csrf => {
    const read = await fetch('/api/state', { credentials: 'include' })
      .then(r => r.text()).catch(() => 'blocked');
    const encoded = btoa("fetch('/api/mutate',{method:'POST',credentials:'include',headers:{'X-CSRF-Token':" + JSON.stringify(csrf) + "}}).then(r=>r.status).catch(()=> 'blocked')");
    const mutation = await (0, eval)(atob(encoded));
    // Also exercise a simple request, which does not need a CORS preflight.
    await fetch('/api/mutate', { method: 'POST', credentials: 'include', body: 'attack' }).catch(() => {});
    return { read, mutation };
  }, csrf);
  expect(result).toEqual({ read: 'blocked', mutation: 'blocked' });
  const state = await page.request.get('/api/state');
  expect((await state.json()).mutations).toBe(0);
});

test('null-origin, missing-origin and spoofed peer requests cannot mutate', async ({ page }) => {
  const csrf = await management(page);
  for (const supplied of ['null', '', 'https://attacker.example']) {
    const response = await page.request.post('/api/mutate', {
      headers: {
        ...(supplied ? { Origin: supplied } : {}),
        'X-CSRF-Token': csrf,
        'X-Drop-Peer': '1',
        'X-Drop-Authorization': 'trusted',
      },
    });
    expect(response.status()).toBe(403);
  }
});

test('direct SVG documents are sandboxed too', async ({ page }) => {
  await management(page);
  const response = await page.goto('/public-123456/hostile.svg');
  expect(response.headers()['content-security-policy']).toContain('sandbox allow-scripts');
  expect(await page.evaluate(() => {
    try { return document.cookie; } catch { return 'blocked'; }
  })).toBe('blocked');
});

test('workers, service workers and management frame access are blocked', async ({ page }) => {
  await management(page);
  await page.goto('/public-123456/');
  const result = await page.evaluate(async () => {
    let worker = 'blocked';
    try {
      const w = new Worker('/public-123456/worker.js');
      worker = await new Promise(resolve => {
        w.onmessage = () => resolve('allowed');
        w.onerror = () => resolve('blocked');
        w.postMessage('test');
        setTimeout(() => resolve('timeout'), 2000);
      });
      w.terminate();
    } catch {}
    let serviceWorker = 'blocked';
    try { await navigator.serviceWorker.register('/public-123456/worker.js'); serviceWorker = 'allowed'; } catch {}
    const frame = document.createElement('iframe');
    const frameAccess = new Promise(resolve => {
      frame.onload = () => {
        try { resolve(frame.contentWindow.document.body.innerText.includes('Management') ? 'allowed' : 'blocked'); }
        catch { resolve('blocked'); }
      };
    });
    frame.src = '/manage'; document.body.append(frame);
    return { worker, serviceWorker, frame: await frameAccess };
  });
  expect(result).toEqual({ worker: 'blocked', serviceWorker: 'blocked', frame: 'blocked' });
});

for (const framework of ['react', 'vue']) {
  test(`prebuilt public ${framework} module bundle is interactive`, async ({ page }) => {
    await page.goto(`/public-123456/${framework}.html`);
    const label = framework === 'react' ? 'React' : 'Vue';
    await expect(page.getByRole('button')).toHaveText(`${label} count: 0`);
    await page.getByRole('button').click();
    await expect(page.getByRole('button')).toHaveText(`${label} count: 1`);
  });
}

test('private documents require authorization and self-contained scripts work', async ({ page, request }) => {
  expect((await request.get('/private-123456/private.html')).status()).toBe(404);
  await management(page, true);
  const response = await page.goto('/private-123456/private.html');
  expect(response.status()).toBe(200);
  expect(response.headers()['access-control-allow-origin']).toBeUndefined();
  expect(response.headers()['cross-origin-resource-policy']).toBe('same-origin');
  await page.getByRole('button').click();
  await expect(page.getByRole('button')).toHaveText('Count: 1');
});

test('attacker cannot read or embed another unlocked private project', async ({ page }) => {
  await management(page, true);
  await page.goto('/public-123456/');
  const result = await page.evaluate(async () => {
    const read = await fetch('/private-123456/private.html', { credentials: 'include' })
      .then(r => r.text()).catch(() => 'blocked');
    const script = await new Promise(resolve => {
      const el = document.createElement('script'); el.src = '/private-123456/probe.js';
      el.onload = () => resolve('loaded'); el.onerror = () => resolve('blocked'); document.body.append(el);
    });
    const image = await new Promise(resolve => {
      const el = new Image(); el.onload = () => resolve('loaded'); el.onerror = () => resolve('blocked');
      el.src = '/private-123456/hostile.svg';
    });
    return { read, script, image, executed: Boolean(window.privateScriptExecuted) };
  });
  expect(result).toEqual({ read: 'blocked', script: 'blocked', image: 'blocked', executed: false });
});

test('public classic scripts and images load as a positive embedding control', async ({ page }) => {
  await page.goto('/public-123456/');
  const result = await page.evaluate(async () => {
    const script = await new Promise(resolve => {
      const el = document.createElement('script'); el.src = '/public-123456/probe.js';
      el.onload = () => resolve('loaded'); el.onerror = () => resolve('blocked'); document.body.append(el);
    });
    const image = await new Promise(resolve => {
      const el = new Image(); el.onload = () => resolve('loaded'); el.onerror = () => resolve('blocked');
      el.src = '/public-123456/hostile.svg';
    });
    return { script, image, executed: Boolean(window.privateScriptExecuted) };
  });
  expect(result).toEqual({ script: 'loaded', image: 'loaded', executed: true });
});

test('private separate-file modules stay blocked instead of relaxing CORS', async ({ page }) => {
  await management(page, true);
  const failed = page.waitForEvent('requestfailed', { predicate: request => request.url().endsWith('/react.js') });
  await page.goto('/private-123456/private-modules.html');
  await failed;
  await expect(page.locator('#app')).toBeEmpty();
  const asset = await page.request.get('/private-123456/react.js', { headers: { Origin: 'null' } });
  expect(asset.status()).toBe(200);
  expect(asset.headers()['access-control-allow-origin']).toBeUndefined();
});

test('HEAD, range, conditional, internal and forged peer paths cannot bypass authorization', async ({ page, request }) => {
  await management(page, true);
  const existing = await page.request.get('/private-123456/probe.js');
  expect(existing.status()).toBe(200);
  for (const headers of [
    { Range: 'bytes=0-10' },
    { 'If-None-Match': existing.headers().etag },
    { 'X-Drop-Peer': '1', 'X-Drop-Authorization': 'trusted' },
  ]) {
    expect((await request.get('/private-123456/probe.js', { headers })).status()).toBe(404);
  }
  expect((await request.head('/private-123456/probe.js')).status()).toBe(404);
  for (const path of ['/_files/private/probe.js', '/_files/public/index.html', '/_files/public/react.js', '/_drop_internal/secret']) {
    expect((await request.get(path)).status()).toBe(404);
  }
  const range = await page.request.get('/private-123456/probe.js', { headers: { Range: 'bytes=0-10' } });
  expect(range.status()).toBe(206);
  expect(range.headers()['content-security-policy']).toContain('sandbox allow-scripts');
  const conditional = await page.request.get('/private-123456/probe.js', { headers: { 'If-None-Match': existing.headers().etag } });
  expect(conditional.status()).toBe(304);
  expect(conditional.headers()['cross-origin-resource-policy']).toBe('same-origin');
});

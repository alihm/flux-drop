(() => {
  'use strict';
  const page = document.getElementById('consent');
  if (!page) return;
  const $ = id => document.getElementById(id);
  const status = $('consent-status');
  let busy = false;
  window.DropAuth.init(JSON.parse(page.dataset.firebase));
  function state() {
    const user = window.DropAuth.current();
    $('consent-login').hidden = Boolean(user);
    $('consent-account').hidden = !user;
    $('consent-user').textContent = user ? 'Signed in as ' + user.email : '';
    $('consent-allow').disabled = busy || !user;
    for (const id of ['consent-google', 'consent-email-submit', 'consent-reset', 'consent-deny', 'consent-switch']) $(id).disabled = busy;
  }
  async function login(action) {
    if (busy) return;
    busy = true; state(); status.textContent = 'Signing in…';
    try { await action(); $('consent-password').value = ''; status.textContent = ''; }
    catch (error) { status.textContent = error.code === 'auth/popup-closed-by-user' ? 'Sign-in cancelled.' : 'Could not sign in. Check your account details and try again.'; }
    finally { busy = false; state(); }
  }
  $('consent-switch').onclick = async () => { if (busy) return; await window.DropAuth.clear(); state(); };
  $('consent-google').onclick = () => login(() => window.DropAuth.google());
  $('consent-email-form').onsubmit = event => { event.preventDefault(); login(() => window.DropAuth.email($('consent-email').value.trim(), $('consent-password').value)); };
  $('consent-reset').onclick = async () => {
    const email = $('consent-email').value.trim();
    if (!email) { status.textContent = 'Enter your email address first.'; $('consent-email').focus(); return; }
    await login(async () => { await window.DropAuth.reset(email); });
    status.textContent = 'If this address has an account, a password reset email will arrive shortly.';
  };
  async function submit(action) {
    if (busy) return;
    busy = true; state(); status.textContent = action === 'allow' ? 'Connecting…' : 'Returning to your agent…';
    try {
      const credentials = action === 'allow' ? await window.DropAuth.credentials() : {};
      const response = await fetch(window.location.pathname, {method: 'POST', credentials: 'same-origin', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({handle: page.dataset.handle, csrf: page.dataset.csrf, action, ...(action === 'allow' ? {idToken: credentials.idToken, refreshToken: credentials.refreshToken} : {})})});
      const result = await response.json();
      if (!response.ok) throw new Error(result.error_description || 'Could not complete the connection.');
      try { await window.DropAuth.clear(); } catch { /* The connection is already saved. */ }
      window.location.assign(result.redirect);
    } catch (error) { status.textContent = error.message; busy = false; state(); }
  }
  $('consent-allow').onclick = () => submit('allow');
  $('consent-deny').onclick = () => submit('deny');
  state();
})();

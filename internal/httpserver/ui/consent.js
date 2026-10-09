(() => {
  'use strict';
  const page = document.getElementById('consent');
  if (!page) return;
  const $ = id => document.getElementById(id);
  const status = $('consent-status');
  let busy = false;
  window.DropAuth.init(JSON.parse(page.dataset.firebase));
  // One line for progress and errors; an error is styled as one so it is not mistaken for progress.
  function say(text, kind = 'info') {
    status.textContent = text;
    status.dataset.kind = text ? kind : '';
  }
  function state() {
    const user = window.DropAuth.current();
    $('consent-login').hidden = Boolean(user);
    $('consent-account').hidden = !user;
    $('consent-user').textContent = user ? 'Signed in as ' + user.email : '';
    $('consent-avatar').textContent = user && user.email ? user.email.charAt(0).toUpperCase() : '';
    $('consent-allow').disabled = busy || !user;
    $('consent-hint').hidden = Boolean(user);
    for (const id of ['consent-google', 'consent-email-submit', 'consent-reset', 'consent-deny', 'consent-switch']) $(id).disabled = busy;
  }
  async function login(action, progress = 'Signing in…', failure = 'Could not sign in. Check your account details and try again.') {
    if (busy) return false;
    busy = true; state(); say(progress);
    try { await action(); $('consent-password').value = ''; say(''); return true; }
    catch (error) { say(error.code === 'auth/popup-closed-by-user' ? 'Sign-in cancelled.' : failure, 'error'); return false; }
    finally { busy = false; state(); }
  }
  $('consent-switch').onclick = async () => { if (busy) return; await window.DropAuth.clear(); say(''); state(); };
  $('consent-google').onclick = () => login(() => window.DropAuth.google());
  $('consent-email-form').onsubmit = event => { event.preventDefault(); login(() => window.DropAuth.email($('consent-email').value.trim(), $('consent-password').value)); };
  $('consent-reset').onclick = async () => {
    const email = $('consent-email').value.trim();
    if (!email) { say('Enter your email address first.', 'error'); $('consent-email').focus(); return; }
    if (await login(() => window.DropAuth.reset(email), 'Sending reset email…', 'Could not send a reset email. Try again in a moment.')) say('If this address has an account, a password reset email will arrive shortly.');
  };
  async function submit(action) {
    if (busy) return;
    busy = true; state(); say(action === 'allow' ? 'Connecting…' : 'Returning to your agent…');
    try {
      const credentials = action === 'allow' ? await window.DropAuth.credentials() : {};
      const response = await fetch(window.location.pathname, {method: 'POST', credentials: 'same-origin', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({handle: page.dataset.handle, csrf: page.dataset.csrf, action, ...(action === 'allow' ? {idToken: credentials.idToken, refreshToken: credentials.refreshToken} : {})})});
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.error_description || 'Could not complete the connection.');
      try { await window.DropAuth.clear(); } catch { /* The connection is already saved. */ }
      say(action === 'allow' ? 'Connected. Returning to your agent; you can close this tab if it stays open.' : 'Returning to your agent…');
      window.location.assign(result.redirect);
    } catch (error) { say(error.message, 'error'); busy = false; state(); }
  }
  $('consent-allow').onclick = () => submit('allow');
  $('consent-deny').onclick = () => submit('deny');
  state();
})();

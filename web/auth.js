import {initializeApp} from 'firebase/app';
import {initializeAuth,inMemoryPersistence,browserPopupRedirectResolver,GoogleAuthProvider,signInWithPopup,signOut} from 'firebase/auth';

let auth;
window.DropAuth = Object.freeze({
  init(config) {
    if (!auth) auth = initializeAuth(initializeApp(config), {persistence:inMemoryPersistence,popupRedirectResolver:browserPopupRedirectResolver});
  },
  async token() {
    if (!auth) throw new Error('Sign-in is not configured.');
    const provider = new GoogleAuthProvider(); provider.setCustomParameters({prompt:'select_account'});
    try { const result = await signInWithPopup(auth,provider); return await result.user.getIdToken(); }
    finally { await signOut(auth); }
  },
  async clear() { if(auth) await signOut(auth); }
});

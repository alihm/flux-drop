import {initializeApp} from 'firebase/app';
import {initializeAuth,inMemoryPersistence,browserPopupRedirectResolver,GoogleAuthProvider,signInWithPopup,signOut,signInWithEmailAndPassword,sendPasswordResetEmail} from 'firebase/auth';

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
  async google() {
    if (!auth) throw new Error('Sign-in is not configured.');
    const provider = new GoogleAuthProvider();
    provider.setCustomParameters({prompt: 'select_account'});
    await signInWithPopup(auth, provider);
  },
  async email(email, password) {
    if (!auth) throw new Error('Sign-in is not configured.');
    await signInWithEmailAndPassword(auth, email, password);
  },
  async reset(email) {
    if (!auth) throw new Error('Sign-in is not configured.');
    await sendPasswordResetEmail(auth, email);
  },
  current() {
    return auth?.currentUser ? {uid: auth.currentUser.uid, email: auth.currentUser.email} : null;
  },
  async credentials() {
    if (!auth?.currentUser) throw new Error('Sign in to continue.');
    const user = auth.currentUser;
    return {uid: user.uid, idToken: await user.getIdToken(true), refreshToken: user.refreshToken};
  },
  async clear() { if(auth) await signOut(auth); }
});

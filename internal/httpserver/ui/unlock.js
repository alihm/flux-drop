"use strict";
(() => {
  const form = document.getElementById("unlock-form");
  const input = document.getElementById("password");
  const button = form.querySelector("button");
  const status = document.getElementById("status");
  const slug = document.getElementById("project").textContent;
  let busy = false;
  form.addEventListener("submit", async event => {
    event.preventDefault();
    if (busy || !form.reportValidity()) return;
    busy = true; button.disabled = true; status.textContent = "Checking access…";
    try {
      const session = await fetch("/api/session", {method: "POST", credentials: "same-origin", cache: "no-store"});
      if (!session.ok) throw new Error(session.status === 429 ? "Too many requests. Wait a minute and try again." : "Your session is unavailable. Please try again later.");
      const data = await session.json();
      if (typeof data.csrfToken !== "string" || !data.csrfToken) throw new Error("Could not establish a secure session.");
      const response = await fetch("/api/unlock", {
        method: "POST", credentials: "same-origin", cache: "no-store",
        headers: {"Content-Type": "application/json", "X-CSRF-Token": data.csrfToken},
        body: JSON.stringify({slug, password: input.value})
      });
      input.value = "";
      if (!response.ok) throw new Error(response.status === 429 ? "Too many attempts. Wait a minute and try again." : response.status === 403 ? "Unable to unlock. Check the password and try again." : "This project is temporarily unavailable. Please try again later.");
      status.textContent = "Unlocked. Opening project…";
      window.location.assign("/" + slug + "/");
    } catch (error) {
      input.value = "";
      status.textContent = error instanceof TypeError ? "Connection interrupted. Please try again." : error.message;
      input.focus();
    } finally {busy = false; button.disabled = false;}
  });
})();

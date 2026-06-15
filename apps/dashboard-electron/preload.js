// Preload runs in a privileged context between Electron and the browser window.
// We don't need to expose anything to the renderer right now,
// but the file needs to exist since main.js references it.
window.addEventListener("DOMContentLoaded", () => {
    console.log("Preload script loaded");
  });
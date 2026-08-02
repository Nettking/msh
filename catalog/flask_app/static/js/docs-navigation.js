(() => {
  const toggle = document.querySelector("[data-docs-nav-toggle]");
  const sidebar = document.querySelector("[data-docs-sidebar]");

  if (!toggle || !sidebar) {
    return;
  }

  const setOpen = (open) => {
    sidebar.classList.toggle("is-open", open);
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
  };

  toggle.addEventListener("click", () => {
    setOpen(toggle.getAttribute("aria-expanded") !== "true");
  });

  sidebar.addEventListener("click", (event) => {
    if (event.target.closest("a") && window.matchMedia("(max-width: 820px)").matches) {
      setOpen(false);
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setOpen(false);
    }
  });

  window.addEventListener("resize", () => {
    if (!window.matchMedia("(max-width: 820px)").matches) {
      setOpen(false);
    }
  });
})();

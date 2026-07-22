(() => {
  const menu = document.querySelector("[data-mobile-navigation]");
  if (!menu) return;

  const summary = menu.querySelector("summary");
  const closeMenu = (restoreFocus = false) => {
    if (!menu.open) return;
    menu.open = false;
    if (restoreFocus) summary?.focus();
  };

  document.addEventListener("click", (event) => {
    if (menu.open && !menu.contains(event.target)) closeMenu();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeMenu(true);
  });

  const desktop = window.matchMedia("(min-width: 821px)");
  const closeAtDesktopWidth = (event) => {
    if (event.matches) closeMenu();
  };
  desktop.addEventListener?.("change", closeAtDesktopWidth);
})();

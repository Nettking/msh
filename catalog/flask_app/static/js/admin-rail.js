/*
 * Left rail behaviour.
 *
 * Two independent states, matching the WordPress admin rail:
 *  - collapsed: a desktop preference, remembered between pages;
 *  - open: the small-screen overlay, always closed on load.
 */
(function () {
  var STORAGE_KEY = "fcp-rail-collapsed";
  var shell = document.querySelector("[data-admin-shell]");
  if (!shell) {
    return;
  }

  var collapseButton = shell.querySelector("[data-rail-collapse]");
  var openButton = shell.querySelector("[data-rail-open]");
  var scrim = shell.querySelector("[data-rail-close]");
  var wide = window.matchMedia("(min-width: 901px)");

  function setCollapsed(collapsed) {
    shell.setAttribute("data-rail-collapsed", collapsed ? "true" : "false");
    if (collapseButton) {
      collapseButton.setAttribute("aria-expanded", collapsed ? "false" : "true");
      var label = collapseButton.querySelector("span");
      if (label) {
        label.textContent = collapsed ? "Expand menu" : "Collapse menu";
      }
    }
    try {
      window.localStorage.setItem(STORAGE_KEY, collapsed ? "true" : "false");
    } catch (error) {
      /* A browser refusing storage still gets a working toggle. */
    }
  }

  function setOpen(open) {
    shell.setAttribute("data-rail-open", open ? "true" : "false");
    if (openButton) {
      openButton.setAttribute("aria-expanded", open ? "true" : "false");
    }
  }

  try {
    if (window.localStorage.getItem(STORAGE_KEY) === "true") {
      setCollapsed(true);
    }
  } catch (error) {
    /* Ignore and start expanded. */
  }

  if (collapseButton) {
    collapseButton.addEventListener("click", function () {
      setCollapsed(shell.getAttribute("data-rail-collapsed") !== "true");
    });
  }

  if (openButton) {
    openButton.addEventListener("click", function () {
      setOpen(shell.getAttribute("data-rail-open") !== "true");
    });
  }

  if (scrim) {
    scrim.addEventListener("click", function () {
      setOpen(false);
    });
  }

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      setOpen(false);
    }
  });

  // Following a link inside the overlay should not leave it open behind the
  // next page paint on browsers that restore scroll position.
  shell.addEventListener("click", function (event) {
    var link = event.target.closest ? event.target.closest("a[href]") : null;
    if (link && !wide.matches) {
      setOpen(false);
    }
  });

  wide.addEventListener("change", function () {
    if (wide.matches) {
      setOpen(false);
    }
  });
})();

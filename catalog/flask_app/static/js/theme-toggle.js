(function () {
  "use strict";

  var STORAGE_KEY = "fcp-theme";
  var root = document.documentElement;

  function systemTheme() {
    return window.matchMedia &&
      window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }

  function storedTheme() {
    try {
      var stored = window.localStorage.getItem(STORAGE_KEY);
      return stored === "dark" || stored === "light" ? stored : null;
    } catch (error) {
      return null;
    }
  }

  function activeTheme() {
    return root.getAttribute("data-theme") || storedTheme() || systemTheme();
  }

  function store(theme) {
    try {
      window.localStorage.setItem(STORAGE_KEY, theme);
    } catch (error) {
      /* Private browsing: the choice simply does not persist. */
    }
  }

  function syncButtons(theme) {
    var buttons = document.querySelectorAll("[data-theme-toggle]");
    var nextLabel = theme === "dark" ? "Switch to light mode" : "Switch to dark mode";
    for (var index = 0; index < buttons.length; index += 1) {
      var button = buttons[index];
      button.setAttribute("aria-pressed", theme === "dark" ? "true" : "false");
      button.setAttribute("title", nextLabel);
      button.setAttribute("aria-label", nextLabel);
      var label = button.querySelector("[data-theme-toggle-label]");
      if (label) {
        label.textContent = theme === "dark" ? "Light mode" : "Dark mode";
      }
    }
  }

  function announce(theme) {
    document.dispatchEvent(
      new CustomEvent("fcp:themechange", { detail: { theme: theme } })
    );
  }

  function apply(theme) {
    root.setAttribute("data-theme", theme);
    syncButtons(theme);
    announce(theme);
  }

  function bind() {
    var buttons = document.querySelectorAll("[data-theme-toggle]");
    for (var index = 0; index < buttons.length; index += 1) {
      buttons[index].addEventListener("click", function () {
        var next = activeTheme() === "dark" ? "light" : "dark";
        store(next);
        apply(next);
      });
    }
    syncButtons(activeTheme());
  }

  // Follow the system while no explicit choice has been stored.
  if (window.matchMedia) {
    var query = window.matchMedia("(prefers-color-scheme: dark)");
    var onChange = function () {
      if (storedTheme()) {
        return;
      }
      root.removeAttribute("data-theme");
      syncButtons(systemTheme());
      announce(systemTheme());
    };
    if (query.addEventListener) {
      query.addEventListener("change", onChange);
    } else if (query.addListener) {
      query.addListener(onChange);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();

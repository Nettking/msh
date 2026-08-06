(function () {
  "use strict";

  function safeParse(value) {
    if (!value) return null;
    try {
      return JSON.parse(value);
    } catch (_error) {
      return null;
    }
  }

  function storageRead(key) {
    if (!key) return null;
    try {
      return safeParse(window.localStorage.getItem(key));
    } catch (_error) {
      return null;
    }
  }

  function storageWrite(key, value) {
    if (!key) return;
    try {
      window.localStorage.setItem(key, JSON.stringify(value));
    } catch (_error) {
      // Storage is an enhancement. Server-rendered progress remains authoritative.
    }
  }

  function storageRemove(key) {
    if (!key) return;
    try {
      window.localStorage.removeItem(key);
    } catch (_error) {
      // Ignore unavailable browser storage.
    }
  }

  function clearOnboardingStorage() {
    try {
      for (var index = window.localStorage.length - 1; index >= 0; index -= 1) {
        var key = window.localStorage.key(index);
        if (key && key.indexOf("msh.onboarding.") === 0) {
          window.localStorage.removeItem(key);
        }
      }
    } catch (_error) {
      // Server-rendered empty state remains authoritative if storage is unavailable.
    }
  }

  function consumeFreshLaunch() {
    try {
      var url = new URL(window.location.href);
      if (url.searchParams.get("fresh") !== "1") return false;
      url.searchParams.delete("fresh");
      url.searchParams.delete("reset");
      window.history.replaceState({}, "", url.pathname + url.search + url.hash);
      return true;
    } catch (_error) {
      return false;
    }
  }

  function initializeShell(shell) {
    var stepButtons = Array.from(shell.querySelectorAll("[data-step-target]"));
    var panels = Array.from(shell.querySelectorAll("[data-step-panel]"));
    var allSteps = stepButtons.map(function (button) { return button.dataset.stepTarget; });
    var availableSteps = stepButtons
      .filter(function (button) { return !button.disabled; })
      .map(function (button) { return button.dataset.stepTarget; });
    var serverStep = shell.dataset.currentStep;
    var revision = shell.dataset.progressRevision || "0";
    var storageKey = shell.dataset.storageKey;
    var stored = storageRead(storageKey);
    var activeStep = serverStep;

    if (
      stored
      && stored.revision === revision
      && availableSteps.indexOf(stored.step) !== -1
      && availableSteps.indexOf(stored.step) >= availableSteps.indexOf(serverStep)
    ) {
      activeStep = stored.step;
    }

    shell.classList.add("is-enhanced");

    function stepIndex(step) {
      return availableSteps.indexOf(step);
    }

    function persist(step) {
      var choices = {};
      shell.querySelectorAll("[data-contribution-choice]:checked").forEach(function (input) {
        choices[input.name] = input.value;
      });
      storageWrite(storageKey, {
        revision: revision,
        step: step,
        choices: choices
      });
    }

    function restoreChoices() {
      if (!stored || stored.revision !== revision || !stored.choices) return;
      Object.keys(stored.choices).forEach(function (name) {
        var selector = "[data-contribution-choice][name=\"" + window.CSS.escape(name) + "\"][value=\"" + window.CSS.escape(stored.choices[name]) + "\"]";
        var input = shell.querySelector(selector);
        if (input && !input.disabled) input.checked = true;
      });
    }

    function updateProgressLabel(step) {
      var label = shell.querySelector("#onboarding-progress-label");
      var button = stepButtons.find(function (item) { return item.dataset.stepTarget === step; });
      if (!label || !button) return;
      var position = allSteps.indexOf(step) + 1;
      var title = button.querySelector("strong");
      label.textContent = "Step " + position + " of " + allSteps.length + " · " + (title ? title.textContent : step);
    }

    function showStep(step, options) {
      options = options || {};
      if (availableSteps.indexOf(step) === -1) step = availableSteps[0];
      activeStep = step;

      panels.forEach(function (panel) {
        var isActive = panel.dataset.stepPanel === step;
        panel.classList.toggle("is-active", isActive);
        panel.setAttribute("aria-hidden", isActive ? "false" : "true");
      });

      stepButtons.forEach(function (button) {
        var isActive = button.dataset.stepTarget === step;
        button.closest(".onboarding-stepper__item").classList.toggle("is-current", isActive);
        if (isActive) button.setAttribute("aria-current", "step");
        else button.removeAttribute("aria-current");
      });

      updateProgressLabel(step);
      persist(step);

      if (options.focus) {
        var panel = panels.find(function (item) { return item.dataset.stepPanel === step; });
        var heading = panel && panel.querySelector("h3");
        if (heading) {
          heading.setAttribute("tabindex", "-1");
          heading.focus({ preventScroll: true });
        }
      }

      if (options.scroll) {
        shell.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }

    function adjacentStep(offset) {
      var index = stepIndex(activeStep);
      var next = availableSteps[index + offset];
      if (next) showStep(next, { focus: true, scroll: true });
    }

    function updateContributionCards() {
      var enabledCount = 0;
      shell.querySelectorAll("[data-contribution-card]").forEach(function (card) {
        var selected = card.querySelector("[data-contribution-choice]:checked");
        var enabled = Boolean(selected && selected.value === "enabled");
        card.classList.toggle("is-selected", enabled);
        if (enabled) enabledCount += 1;
      });
      var count = shell.querySelector("[data-selection-count]");
      if (count) {
        count.textContent = enabledCount === 1 ? "1 contribution enabled" : enabledCount + " contributions enabled";
      }
      persist(activeStep);
    }

    stepButtons.forEach(function (button) {
      button.addEventListener("click", function () {
        showStep(button.dataset.stepTarget, { focus: true, scroll: true });
      });
    });

    shell.querySelectorAll("[data-next-step]").forEach(function (button) {
      button.addEventListener("click", function () { adjacentStep(1); });
    });

    shell.querySelectorAll("[data-prev-step]").forEach(function (button) {
      button.addEventListener("click", function () { adjacentStep(-1); });
    });

    shell.querySelectorAll("[data-contribution-choice]").forEach(function (input) {
      input.addEventListener("change", updateContributionCards);
    });

    shell.querySelectorAll("[data-benchmark-form]").forEach(function (form) {
      form.addEventListener("submit", function () {
        var card = form.closest("[data-benchmark-card]");
        var status = card && card.querySelector("[role=status]");
        var button = form.querySelector("button[type=submit]");
        if (button) {
          button.disabled = true;
          button.setAttribute("aria-busy", "true");
          button.textContent = "Starting…";
        }
        if (status) status.textContent = "Starting bounded check…";
      });
    });

    shell.querySelectorAll("[data-finish-link]").forEach(function (link) {
      link.addEventListener("click", function () { storageRemove(storageKey); });
    });

    restoreChoices();
    updateContributionCards();
    showStep(activeStep, { focus: false, scroll: false });
  }

  if (consumeFreshLaunch()) clearOnboardingStorage();
  document.querySelectorAll("[data-onboarding-shell]").forEach(initializeShell);
})();

(function () {
  "use strict";

  // Chart.js paints its own defaults (dark grey text, near-black grid lines),
  // which disappear on a dark surface. Bind those defaults to the theme tokens
  // and re-apply them whenever the palette changes.
  if (typeof window.Chart === "undefined") {
    return;
  }

  function token(name, fallback) {
    var value = getComputedStyle(document.documentElement)
      .getPropertyValue(name)
      .trim();
    return value || fallback;
  }

  function applyDefaults() {
    var Chart = window.Chart;
    Chart.defaults.color = token("--text-muted", "#475569");
    Chart.defaults.borderColor = token("--border", "#dae1ec");
    Chart.defaults.font.family = token(
      "--font-sans",
      'Inter, system-ui, "Segoe UI", Roboto, Arial, sans-serif'
    );
    if (Chart.defaults.plugins && Chart.defaults.plugins.legend) {
      Chart.defaults.plugins.legend.labels.color = token("--text", "#0f172a");
    }
    if (Chart.defaults.plugins && Chart.defaults.plugins.tooltip) {
      Chart.defaults.plugins.tooltip.backgroundColor = token("--text", "#0f172a");
    }
  }

  // Chart.js caches resolved scale options, so changing the defaults alone
  // leaves an existing chart on its old palette. Write the colours into the
  // raw config (chart.config.options) and update. Never write through
  // chart.options: in v4 that is a resolving proxy and assigning to it
  // recurses until the stack blows.
  function refreshExistingCharts() {
    var Chart = window.Chart;
    if (typeof Chart.getChart !== "function") {
      return;
    }
    var canvases = document.querySelectorAll("canvas");
    for (var index = 0; index < canvases.length; index += 1) {
      var chart = Chart.getChart(canvases[index]);
      if (!chart || !chart.config) {
        continue;
      }
      var options = chart.config.options || {};
      options.color = Chart.defaults.color;
      Object.keys(options.scales || {}).forEach(function (key) {
        var scale = options.scales[key];
        scale.ticks = scale.ticks || {};
        scale.ticks.color = Chart.defaults.color;
        scale.grid = scale.grid || {};
        scale.grid.color = Chart.defaults.borderColor;
        if (scale.title) {
          scale.title.color = Chart.defaults.color;
        }
      });
      chart.config.options = options;
      chart.update("none");
    }
  }

  applyDefaults();

  document.addEventListener("fcp:themechange", function () {
    applyDefaults();
    refreshExistingCharts();
  });
})();

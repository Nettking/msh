(function () {
  "use strict";

  // Chart.js paints its own defaults (dark grey text, near-black grid lines),
  // which disappear on a dark surface. Bind those defaults to the theme tokens
  // and re-apply them whenever the palette changes.
  if (typeof window.Chart === "undefined") {
    return;
  }

  var LEGACY_SERIES_INDEX = {
    "#1d4ed8": 0,
    "#15803d": 1,
    "#a16207": 2,
    "#7c3aed": 3,
    "#be123c": 4,
    "#0891b2": 5,
    "#4338ca": 6,
    "#0f766e": 7
  };

  function token(name, fallback) {
    var value = getComputedStyle(document.documentElement)
      .getPropertyValue(name)
      .trim();
    return value || fallback;
  }

  function seriesPalette() {
    return [
      token("--chart-1", "#1d4ed8"),
      token("--chart-2", "#15803d"),
      token("--chart-3", "#a16207"),
      token("--chart-4", "#7c3aed"),
      token("--chart-5", "#be123c"),
      token("--chart-6", "#0891b2"),
      token("--chart-7", "#4338ca"),
      token("--chart-8", "#0f766e")
    ];
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
      Chart.defaults.plugins.tooltip.backgroundColor = token("--tooltip-bg", "#0f172a");
      Chart.defaults.plugins.tooltip.titleColor = token("--on-solid", "#ffffff");
      Chart.defaults.plugins.tooltip.bodyColor = token("--on-solid", "#ffffff");
      Chart.defaults.plugins.tooltip.footerColor = token("--on-solid", "#ffffff");
    }
  }

  function refreshSeries(chart) {
    if (!chart.config || !chart.config.data || !Array.isArray(chart.config.data.datasets)) {
      return;
    }
    var palette = seriesPalette();
    chart.config.data.datasets.forEach(function (dataset) {
      var paletteIndex = Number.isInteger(dataset._fcpThemeSeriesIndex)
        ? dataset._fcpThemeSeriesIndex
        : null;
      if (paletteIndex === null && typeof dataset.borderColor === "string") {
        var legacyIndex = LEGACY_SERIES_INDEX[dataset.borderColor.toLowerCase()];
        if (Number.isInteger(legacyIndex)) {
          paletteIndex = legacyIndex;
          dataset._fcpThemeSeriesIndex = legacyIndex;
        }
      }
      if (paletteIndex !== null) {
        dataset.borderColor = palette[paletteIndex % palette.length];
      }
    });
  }

  // Chart.js caches resolved options, so changing defaults alone can leave an
  // existing chart on its old palette. Write colours into raw config and update.
  // Never write through chart.options: in v4 that is a resolving proxy and
  // assigning to it recurses until the stack blows.
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
      if (Chart.defaults.plugins && Chart.defaults.plugins.tooltip) {
        options.plugins = options.plugins || {};
        options.plugins.tooltip = options.plugins.tooltip || {};
        options.plugins.tooltip.backgroundColor = Chart.defaults.plugins.tooltip.backgroundColor;
        options.plugins.tooltip.titleColor = Chart.defaults.plugins.tooltip.titleColor;
        options.plugins.tooltip.bodyColor = Chart.defaults.plugins.tooltip.bodyColor;
        options.plugins.tooltip.footerColor = Chart.defaults.plugins.tooltip.footerColor;
      }
      refreshSeries(chart);
      chart.config.options = options;
      chart.update("none");
    }
  }

  applyDefaults();

  // chart-theme.js is loaded in the head before inline page charts are built.
  // Revisit them after DOMContentLoaded so initial Playback charts also receive
  // the theme-aware series palette rather than waiting for the first toggle.
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", refreshExistingCharts);
  } else {
    refreshExistingCharts();
  }

  document.addEventListener("fcp:themechange", function () {
    applyDefaults();
    refreshExistingCharts();
  });
})();

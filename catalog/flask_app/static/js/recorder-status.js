(() => {
  const dashboard = document.querySelector("[data-recorder-dashboard]");
  if (!dashboard) {
    return;
  }

  const statusUrl = dashboard.dataset.statusUrl;
  const stateBadge = dashboard.querySelector("[data-recorder-state]");
  const message = dashboard.querySelector("[data-recorder-message]");
  const liveState = dashboard.querySelector("[data-recorder-live-state]");
  const recordsWritten = dashboard.querySelector("[data-recorder-records]");
  const lastFlush = dashboard.querySelector("[data-recorder-last-flush]");
  const sourceList = dashboard.querySelector("[data-recorder-sources]");
  const controlForm = dashboard.querySelector("[data-recorder-control]");
  const controlButton = dashboard.querySelector(
    "[data-recorder-control-button]",
  );
  const numberFormatter = new Intl.NumberFormat();
  const timeFormatter = new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  const requestTimeoutMs = 8000;
  const stateLabels = {
    caught_up: "Caught up",
    error: "Error",
    not_configured: "Not configured",
    offline: "Offline",
    polling: "Polling",
    recording: "Recording",
    starting: "Starting",
    stopped: "Stopped",
    waiting: "Waiting",
  };

  let timer = null;
  let inFlight = false;
  let failures = 0;
  let stopped = false;
  let controller = null;
  let pollAfterMs = 2000;

  const text = (value) => String(value ?? "");

  const formattedNumber = (value, fallback = "Waiting") => {
    const parsed = Number(value);
    return Number.isFinite(parsed)
      ? numberFormatter.format(Math.max(0, Math.trunc(parsed)))
      : fallback;
  };

  const timestampText = (value, fallback = "Waiting") => {
    if (!value) {
      return fallback;
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return text(value);
    }
    const secondsAgo = Math.max(
      0,
      Math.round((Date.now() - parsed.getTime()) / 1000),
    );
    if (secondsAgo < 5) {
      return "Just now";
    }
    if (secondsAgo < 60) {
      return `${secondsAgo}s ago`;
    }
    if (secondsAgo < 3600) {
      return `${Math.floor(secondsAgo / 60)}m ago`;
    }
    return timeFormatter.format(parsed);
  };

  const setTimestamp = (element, value, fallback) => {
    if (!element) {
      return;
    }
    element.textContent = timestampText(value, fallback);
    element.dataset.timestamp = text(value);
    const parsed = value ? new Date(value) : null;
    element.title =
      parsed && !Number.isNaN(parsed.getTime()) ? parsed.toLocaleString() : "";
  };

  const stateClass = (state) => {
    if (["recording", "caught_up", "polling"].includes(state)) {
      return "status-ready";
    }
    if (["error", "offline"].includes(state)) {
      return "status-error";
    }
    return "status-waiting";
  };

  const setStateBadge = (element, state) => {
    if (!element) {
      return;
    }
    element.classList.remove(
      "status-ready",
      "status-error",
      "status-waiting",
    );
    element.classList.add(stateClass(state));
    const label =
      stateLabels[state] ||
      text(state).replaceAll("_", " ") ||
      stateLabels.waiting;
    if (element.textContent.trim() !== label) {
      element.textContent = label;
    }
  };

  const setLiveState = (label, delayed, title = "") => {
    if (!liveState) {
      return;
    }
    if (liveState.textContent !== label) {
      liveState.textContent = label;
    }
    liveState.classList.toggle("is-delayed", delayed);
    liveState.title = title;
  };

  const makeElement = (tagName, className = "") => {
    const element = document.createElement(tagName);
    if (className) {
      element.className = className;
    }
    return element;
  };

  const createMachineCard = (sourceName) => {
    const card = makeElement("article", "recorder-machine");
    card.dataset.sourceName = sourceName;

    const header = makeElement("header");
    const identity = makeElement("div");
    const title = makeElement("h4");
    title.dataset.sourceTitle = "";
    const machineSummary = makeElement("span");
    machineSummary.dataset.machineSummary = "";
    identity.append(title, machineSummary);
    const badge = makeElement("span", "status-badge status-waiting");
    badge.dataset.sourceState = "";
    header.append(identity, badge);

    const facts = makeElement("dl", "recorder-machine__facts");
    const sequenceFact = makeElement("div");
    const sequenceLabel = makeElement("dt");
    sequenceLabel.textContent = "Next sequence";
    const sequenceValue = makeElement("dd");
    sequenceValue.dataset.nextSequence = "";
    sequenceFact.append(sequenceLabel, sequenceValue);
    const timeFact = makeElement("div");
    const timeLabel = makeElement("dt");
    timeLabel.textContent = "Last data";
    const timeValue = makeElement("dd");
    timeValue.dataset.lastData = "";
    timeFact.append(timeLabel, timeValue);
    facts.append(sequenceFact, timeFact);

    const details = makeElement("details");
    const summary = makeElement("summary");
    summary.textContent = "Connection details";
    const detailList = makeElement("dl");

    const agentRow = makeElement("div");
    const agentLabel = makeElement("dt");
    agentLabel.textContent = "Agent";
    const agentValue = makeElement("dd");
    const agentCode = makeElement("code");
    agentCode.dataset.agentUrl = "";
    agentValue.append(agentCode);
    agentRow.append(agentLabel, agentValue);

    const machineRow = makeElement("div");
    const machineLabel = makeElement("dt");
    machineLabel.textContent = "Machine ID";
    const machineValue = makeElement("dd");
    const machineCode = makeElement("code");
    machineCode.dataset.machineId = "";
    machineValue.append(machineCode);
    machineRow.append(machineLabel, machineValue);

    const errorRow = makeElement("div");
    errorRow.dataset.sourceErrorRow = "";
    const errorLabel = makeElement("dt");
    errorLabel.textContent = "Error";
    const errorValue = makeElement("dd");
    errorValue.dataset.sourceError = "";
    errorRow.append(errorLabel, errorValue);

    detailList.append(agentRow, machineRow, errorRow);
    details.append(summary, detailList);
    card.append(header, facts, details);
    return card;
  };

  const updateMachineCard = (card, source) => {
    const sourceName = text(source.source_name) || "Unknown source";
    const machineId = text(source.machine_id);
    card.dataset.sourceName = sourceName;
    card.querySelector("[data-source-title]").textContent = sourceName;

    const machineSummary = card.querySelector("[data-machine-summary]");
    machineSummary.textContent =
      machineId && machineId !== sourceName ? machineId : "";
    machineSummary.hidden = !machineSummary.textContent;

    setStateBadge(
      card.querySelector("[data-source-state]"),
      text(source.state) || "waiting",
    );
    card.querySelector("[data-next-sequence]").textContent = formattedNumber(
      source.next_sequence,
    );
    setTimestamp(
      card.querySelector("[data-last-data]"),
      source.last_success_at,
      "Waiting",
    );
    card.querySelector("[data-agent-url]").textContent =
      text(source.base_url) || "Waiting for recorder";
    card.querySelector("[data-machine-id]").textContent =
      machineId || "Waiting for data";

    const error = text(source.last_error);
    const errorRow = card.querySelector("[data-source-error-row]");
    errorRow.hidden = !error;
    card.querySelector("[data-source-error]").textContent = error;
  };

  const renderSources = (sources) => {
    if (!sourceList) {
      return;
    }
    const normalizedSources = Array.isArray(sources) ? sources : [];
    const existing = new Map(
      [...sourceList.querySelectorAll("[data-source-name]")].map((card) => [
        card.dataset.sourceName,
        card,
      ]),
    );
    sourceList
      .querySelectorAll(".recorder-machine-list__empty")
      .forEach((element) => element.remove());

    normalizedSources.forEach((source) => {
      const sourceName = text(source.source_name) || "Unknown source";
      let card = existing.get(sourceName);
      if (!card || !card.querySelector("[data-source-state]")) {
        const wasOpen = Boolean(card?.querySelector("details[open]"));
        card?.remove();
        card = createMachineCard(sourceName);
        card.querySelector("details").open = wasOpen;
      }
      updateMachineCard(card, source);
      sourceList.append(card);
      existing.delete(sourceName);
    });
    existing.forEach((card) => card.remove());

    if (!normalizedSources.length) {
      const empty = makeElement("p", "recorder-machine-list__empty");
      empty.textContent = "No machines are configured yet.";
      sourceList.append(empty);
    }
  };

  const renderStatus = (payload) => {
    setStateBadge(stateBadge, text(payload.state) || "waiting");
    if (message) {
      message.textContent = text(payload.message);
    }
    if (recordsWritten) {
      recordsWritten.textContent = formattedNumber(payload.records_written, "0");
    }
    setTimestamp(lastFlush, payload.last_flush_at, "Waiting for data");
    renderSources(payload.sources);

    if (controlForm && controlButton) {
      const requested = Boolean(payload.requested_enabled);
      controlForm.action = requested
        ? controlForm.dataset.stopUrl
        : controlForm.dataset.startUrl;
      controlButton.textContent = requested
        ? "Stop recording"
        : "Start recording";
    }
  };

  const schedule = (delay) => {
    window.clearTimeout(timer);
    if (!stopped) {
      timer = window.setTimeout(poll, delay);
    }
  };

  const poll = async () => {
    if (stopped || inFlight) {
      return;
    }
    if (document.hidden) {
      schedule(pollAfterMs);
      return;
    }

    inFlight = true;
    controller = new AbortController();
    let timedOut = false;
    const requestTimeout = window.setTimeout(() => {
      timedOut = true;
      controller?.abort();
    }, requestTimeoutMs);
    try {
      const response = await fetch(statusUrl, {
        cache: "no-store",
        headers: { Accept: "application/json" },
        signal: controller.signal,
      });
      if (!response.ok) {
        throw new Error(`Recorder status returned ${response.status}`);
      }
      const payload = await response.json();
      if (payload?.schema !== "msh.recorder.web_status.v1") {
        throw new Error("Recorder status returned an unexpected response");
      }
      renderStatus(payload);
      pollAfterMs = Math.max(1000, Number(payload.poll_after_ms) || 2000);
      failures = 0;
      setLiveState(
        "Live",
        false,
        `Updated ${new Date().toLocaleTimeString()}`,
      );
    } catch (error) {
      const aborted =
        error instanceof DOMException && error.name === "AbortError";
      if (timedOut || !aborted) {
        failures += 1;
        setLiveState("Updates delayed — retrying", true);
      }
    } finally {
      window.clearTimeout(requestTimeout);
      inFlight = false;
      controller = null;
      const retryDelay = failures
        ? Math.min(10000, pollAfterMs * 2 ** Math.min(failures, 3))
        : pollAfterMs;
      schedule(retryDelay);
    }
  };

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      schedule(0);
    }
  });
  window.addEventListener("pageshow", () => {
    if (stopped) {
      stopped = false;
      schedule(0);
    }
  });
  window.addEventListener(
    "pagehide",
    () => {
      stopped = true;
      window.clearTimeout(timer);
      controller?.abort();
    },
  );

  poll();
})();

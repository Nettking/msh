(() => {
  const form = document.querySelector("[data-upload-form]");
  const input = document.querySelector("[data-upload-input]");
  const dropzone = document.querySelector("[data-upload-dropzone]");
  const selection = document.querySelector("[data-upload-selection]");
  const list = document.querySelector("[data-upload-file-list]");
  const count = document.querySelector("[data-upload-count]");
  const size = document.querySelector("[data-upload-size]");
  const submit = document.querySelector("[data-upload-submit]");

  const formatBytes = (bytes) => {
    if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
    const units = ["B", "KB", "MB", "GB"];
    const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const value = bytes / Math.pow(1024, index);
    return `${value.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
  };

  const renderFiles = () => {
    if (!input || !selection || !list || !count || !size || !submit) return;
    const files = Array.from(input.files || []);
    list.replaceChildren();
    let total = 0;
    let valid = files.length > 0;
    files.forEach((file) => {
      total += file.size;
      if (!file.name.toLowerCase().endsWith(".jsonl")) valid = false;
      const item = document.createElement("li");
      const name = document.createElement("strong");
      const meta = document.createElement("span");
      name.textContent = file.name;
      meta.textContent = formatBytes(file.size);
      item.append(name, meta);
      list.append(item);
    });
    selection.hidden = files.length === 0;
    count.textContent = `${files.length} file${files.length === 1 ? "" : "s"} selected`;
    size.textContent = formatBytes(total);
    submit.disabled = !valid;
  };

  if (input) input.addEventListener("change", renderFiles);

  if (dropzone && input) {
    ["dragenter", "dragover"].forEach((eventName) => {
      dropzone.addEventListener(eventName, (event) => {
        event.preventDefault();
        dropzone.classList.add("is-dragging");
      });
    });
    ["dragleave", "drop"].forEach((eventName) => {
      dropzone.addEventListener(eventName, (event) => {
        event.preventDefault();
        dropzone.classList.remove("is-dragging");
      });
    });
    dropzone.addEventListener("drop", (event) => {
      const dropped = Array.from(event.dataTransfer?.files || []);
      if (!dropped.length) return;
      const transfer = new DataTransfer();
      dropped.forEach((file) => transfer.items.add(file));
      input.files = transfer.files;
      renderFiles();
    });
  }

  if (form && submit) {
    form.addEventListener("submit", () => {
      submit.disabled = true;
      submit.textContent = "Uploading…";
      dropzone?.classList.add("is-uploading");
    });
  }

  const statusPanel = document.querySelector("[data-upload-status]");
  if (!statusPanel) return;
  const statusUrl = statusPanel.dataset.statusUrl;
  const initialStatus = statusPanel.dataset.initialStatus;
  if (!statusUrl || ["ready", "failed"].includes(initialStatus || "")) return;

  let stopped = false;
  const poll = async () => {
    if (stopped) return;
    try {
      const response = await fetch(statusUrl, {
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok || !payload.batch) throw new Error("status unavailable");
      const batch = payload.batch;
      const label = document.querySelector("[data-upload-status-label]");
      const records = document.querySelector("[data-upload-record-count]");
      if (label) label.textContent = String(batch.status || "working").replaceAll("-", " ");
      if (records) records.textContent = String(batch.imported_records || 0);
      if (["ready", "failed"].includes(batch.status) || batch.status !== initialStatus) {
        stopped = true;
        window.location.reload();
        return;
      }
    } catch (_error) {
      const message = document.querySelector("[data-upload-live-message]");
      if (message) message.textContent = "Waiting for the import worker. The page will retry automatically.";
    }
    window.setTimeout(poll, 1500);
  };
  window.setTimeout(poll, 800);
})();

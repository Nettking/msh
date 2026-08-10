document.addEventListener("DOMContentLoaded", () => {
  const formatter = new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  });

  document.querySelectorAll("time[data-local-datetime]").forEach((element) => {
    const raw = element.getAttribute("datetime");
    if (!raw) return;

    const value = new Date(raw);
    if (Number.isNaN(value.getTime())) return;

    element.textContent = formatter.format(value);
    element.title = `${raw} (UTC source time)`;
  });
});

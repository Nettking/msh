(() => {
  const chat = document.querySelector("[data-ai-chat]");
  if (!chat) return;

  const form = chat.querySelector("[data-ai-form]");
  const transcript = chat.querySelector("[data-ai-transcript]");
  const question = chat.querySelector("[data-ai-question]");
  const send = chat.querySelector("[data-ai-send]");
  const provider = chat.dataset.provider || "the configured model";

  const scrollToMessage = (message) => {
    message.scrollIntoView({ behavior: "smooth", block: "nearest" });
  };

  const createMessage = (role, className = "") => {
    const message = document.createElement("article");
    message.className = `ai-message ai-message--${role} ${className}`.trim();

    const roleLabel = document.createElement("div");
    roleLabel.className = "ai-message__role";
    roleLabel.textContent = role === "user" ? "You" : "FCP";

    const bubble = document.createElement("div");
    bubble.className = "ai-message__bubble";
    message.append(roleLabel, bubble);
    transcript.append(message);
    return { message, bubble };
  };

  const appendUserMessage = (text) => {
    const { message, bubble } = createMessage("user");
    const paragraph = document.createElement("p");
    paragraph.textContent = text;
    bubble.append(paragraph);
    scrollToMessage(message);
  };

  const appendPendingMessage = () => {
    const { message, bubble } = createMessage("assistant", "ai-message--pending");
    message.setAttribute("role", "status");
    message.setAttribute("aria-label", `FCP is working with ${provider}`);

    const thinking = document.createElement("div");
    thinking.className = "ai-thinking";
    const label = document.createElement("span");
    label.textContent = `Working with ${provider}`;
    const dots = document.createElement("span");
    dots.className = "ai-thinking__dots";
    dots.setAttribute("aria-hidden", "true");
    dots.append(document.createElement("i"), document.createElement("i"), document.createElement("i"));
    thinking.append(label, dots);
    bubble.append(thinking);
    scrollToMessage(message);
    return message;
  };

  const addEvidence = (bubble, sources, context) => {
    if ((!sources || sources.length === 0) && !context) return;

    const evidence = document.createElement("details");
    evidence.className = "ai-chat-evidence";
    const summary = document.createElement("summary");
    const label = document.createElement("span");
    label.textContent = "Inspect sources";
    summary.append(label);

    if (sources && sources.length > 0) {
      const count = document.createElement("span");
      count.className = "ai-chat-evidence__count";
      count.textContent = String(sources.length);
      summary.append(count);
    }

    const body = document.createElement("div");
    body.className = "ai-chat-evidence__body";
    if (sources && sources.length > 0) {
      const list = document.createElement("ul");
      list.className = "ai-chat-source-list";
      sources.forEach((source) => {
        const item = document.createElement("li");
        const code = document.createElement("code");
        code.textContent = source;
        item.append(code);
        list.append(item);
      });
      body.append(list);
    }

    if (context) {
      const contextDetails = document.createElement("details");
      contextDetails.className = "ai-chat-context";
      const contextSummary = document.createElement("summary");
      contextSummary.textContent = "View retrieved excerpts";
      const contextPre = document.createElement("pre");
      contextPre.textContent = context;
      contextDetails.append(contextSummary, contextPre);
      body.append(contextDetails);
    }

    evidence.append(summary, body);
    bubble.append(evidence);
  };

  const replacePending = (pending, payload) => {
    const { message, bubble } = createMessage(
      "assistant",
      payload.error ? "ai-message--error" : "",
    );
    const answer = document.createElement("div");
    answer.className = "ai-answer";

    if (payload.error) {
      const paragraph = document.createElement("p");
      paragraph.textContent = payload.error;
      answer.append(paragraph);
    } else if (payload.answer_html) {
      answer.innerHTML = payload.answer_html;
    } else {
      const paragraph = document.createElement("p");
      paragraph.textContent = "Retrieved context is ready. Open Inspect sources to review it.";
      answer.append(paragraph);
    }

    bubble.append(answer);
    addEvidence(bubble, payload.sources, payload.context);
    pending.replaceWith(message);
    scrollToMessage(message);
  };

  const autoSize = () => {
    question.style.height = "auto";
    question.style.height = `${Math.min(question.scrollHeight, 180)}px`;
  };

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const text = question.value.trim();
    if (!text || form.getAttribute("aria-busy") === "true") return;

    const formData = new FormData(form);
    appendUserMessage(text);
    question.value = "";
    autoSize();
    const pending = appendPendingMessage();
    form.setAttribute("aria-busy", "true");
    send.disabled = true;
    question.disabled = true;

    try {
      const response = await fetch(form.action, {
        method: "POST",
        body: formData,
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      replacePending(pending, payload);
    } catch (_error) {
      replacePending(pending, {
        error: "The explainer could not complete the request. Check the model connection and try again.",
        sources: [],
        context: "",
      });
    } finally {
      form.removeAttribute("aria-busy");
      send.disabled = false;
      question.disabled = false;
      question.focus();
    }
  });

  question.addEventListener("input", autoSize);
  question.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      form.requestSubmit();
    }
  });

  chat.querySelectorAll("[data-ai-suggestion]").forEach((suggestion) => {
    suggestion.addEventListener("click", () => {
      question.value = suggestion.dataset.aiSuggestion || "";
      autoSize();
      form.requestSubmit();
    });
  });
})();

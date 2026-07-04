# AI Explainer

This document defines the first, read-only AI integration for MSH. The goal is to let a local Ollama-backed assistant explain how the system works without changing code, running operational actions, or inspecting raw telemetry by default.

## Purpose

The AI explainer should answer system-understanding questions about the MSH codebase and documentation, for example:

- how telemetry flows from raw JSONL data into workflow sessions, derived artifacts, playback exports, and Flask views
- what the main Flask routes are responsible for
- how the runner, orchestrator, common telemetry utilities, and cache layer relate to each other
- where outputs are stored and which artifacts are generated
- which parts of the repository are current workflow paths and which parts are legacy or exploratory

The explainer is not intended to be an autonomous developer agent in the first version. It should not modify files, create commits, run scripts, delete artifacts, rebuild caches, or make operational decisions.

## Operating mode

The first implementation should use retrieval-augmented generation:

1. Index selected repository files.
2. Split indexed files into small chunks with file path and line metadata.
3. Retrieve the most relevant chunks for a user question.
4. Send only the question and retrieved context to the local Ollama model.
5. Return an answer that cites the repository files used as context.

The model should be treated as an explanation layer over retrieved repository context, not as the source of truth.

## Default indexed sources

The initial index should include:

```text
README.md
docs/**/*.md
catalog/**/*.py
catalog/**/README.md
docker-compose.yml
Dockerfile
requirements.txt
```

These files cover the documented architecture, setup instructions, Flask application, orchestration/runtime logic, runner/session logic, shared telemetry utilities, and deployment assumptions.

## Default exclusions

The initial index should exclude:

```text
data/**
results/**
legacy/**
.git/**
__pycache__/**
*.pyc
*.parquet
*.duckdb
*.jsonl
```

`data/` and `results/` may contain large generated or local runtime artifacts. Raw telemetry should remain outside the default AI context unless a later, explicit analysis workflow is designed for it. `legacy/` should be excluded by default because it is not part of the current workflow path.

Small committed sample data can be considered later, but it should not be indexed in the first version unless the explainer needs concrete examples to explain the data contract.

## Safety boundaries

The first version must be read-only:

- no file writes
- no commits or branch changes
- no Docker commands
- no script execution
- no cache rebuilds
- no deletion of `data/` or `results/`
- no use of raw telemetry unless explicitly enabled later

If a user asks for an operational action, the explainer should describe where that action is documented or implemented, but it should not perform the action.

## Suggested package layout

A minimal implementation can be placed under:

```text
catalog/ai/
  __init__.py
  repo_index.py
  rag.py
  ollama_client.py
  prompts.py
```

Responsibilities:

- `repo_index.py`: discover allowed files, apply exclusions, read text, split chunks, and store a local index
- `rag.py`: retrieve relevant chunks for a question
- `ollama_client.py`: call the local Ollama API
- `prompts.py`: keep the read-only system prompt and answer-format instructions

A Flask route can be added later, for example:

```text
catalog/flask_app/routes/ai.py
catalog/flask_app/templates/ai.html
```

## Answer style

The explainer should give short, practical answers grounded in retrieved context. It should make uncertainty visible when the available context is incomplete. Answers should include file references so a developer can inspect the relevant implementation or documentation.

## Recommended first questions for testing

Use these questions to validate the first implementation:

```text
How does data flow through MSH?
What does the /control page do?
What is the difference between raw JSONL and the telemetry analytics cache?
Where are workflow session artifacts stored?
Which parts of the repository are legacy?
What happens during startup?
```

## Model choice

For code and system explanation, prefer a coder-oriented model when available, such as `qwen2.5-coder:7b`. Smaller models can be used for lightweight testing, but they may produce weaker explanations and should not be trusted without retrieved context.

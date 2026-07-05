# MSH workflow-oriented navigation

The app should not expose every implementation page in the main menu. The menu should guide the operator through the real workflow.

## Top-level areas

### Monitor

For current machine/system use.

Primary pages:

- Overview
- Live
- Playback
- Assist

Purpose:

```text
What is happening now?
What can I inspect now?
What support does the operator need now?
```

### Workflow

For turning field experience into structured operator strategies and technical intervention logic.

Primary pages:

- Capture
- Review Notes
- Strategies
- Intervention Logic
- SysML Export

Workflow:

```text
1. Capture a raw statement on site.
2. Later review the note.
3. Structure it into OSL/paper keywords.
4. Compare structured notes as candidate strategies.
5. Define intervention logic only when a telemetry condition can detect the situation.
6. Mark good strategies reusable.
7. Export reusable strategies to SysML.
```

Important distinction:

```text
Raw statement        = what was said or observed on site.
Structured strategy  = interpreted operator knowledge using OSL/paper fields.
Intervention logic   = technical YAML rule for detecting candidate situations from telemetry.
SysML export         = formal model handoff for reusable strategies.
```

### System

For configuration, documentation, and troubleshooting.

Primary pages:

- Guide
- Setup
- Sources
- Diagnostics

Purpose:

```text
Guide       = how to use the app.
Setup       = how this MSH instance should run.
Sources     = what machines and sensors exist.
Diagnostics = why the runtime/data pipeline is or is not ready.
```

Detailed/advanced pages should be reachable from cards on these pages, not dumped into the main menu.

## Design rule

Every main page should answer three questions:

1. What is this page for?
2. What should I do here?
3. Where does this step fit in the workflow?

## In-app documentation

MSH should include a user-facing guide page inside the app. This is different from repo documentation:

- Repo docs explain design decisions for developers/researchers.
- The in-app guide explains what the operator or researcher should click next.

The in-app guide should stay short, concrete, and workflow-based. It should avoid implementation details unless they help the user decide what to do next.

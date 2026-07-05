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

### Workflow

For turning field experience into structured operator strategies.

Primary pages:

- Capture
- Review Notes
- Strategies
- SysML Export

Workflow:

```text
1. Capture a raw statement on site.
2. Later review the note.
3. Structure it into OSL/paper keywords.
4. Mark it reusable when the interpretation is good enough.
5. Export reusable strategies to SysML.
```

### System

For configuration and troubleshooting.

Primary pages:

- Setup
- Sources
- Diagnostics

Detailed/advanced pages should be reachable from cards on these pages, not from the main menu.

## Design rule

Every main page should answer three questions:

1. What is this page for?
2. What should I do here?
3. Where does this step fit in the workflow?

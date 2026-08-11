# FCP knowledge-oriented navigation

The app should not expose every implementation page in the main menu. The menu should guide the user through the real knowledge flow.

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

### Knowledge

For turning field experience into structured operator strategies and technical intervention logic.

Primary pages:

- Capture
- Review Notes
- Strategies
- Intervention Logic
- SysML Export

Knowledge flow:

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

For configuration, documentation, source setup, and troubleshooting.

Primary pages:

- Guide
- Setup
- Sources
- Diagnostics

Purpose:

```text
Guide       = how to use the app.
Setup       = how this FCP instance should run.
Sources     = machines, sensors, MTConnect endpoints, Observer Phoenix, and connection tests.
Diagnostics = why the runtime/data pipeline is or is not ready.
```

System -> Sources includes machine-level connection checks:

```text
Test MTConnect    = HTTP test from Flask server/container to the machine adapter endpoint.
Test VPN/network  = TCP reachability test from Flask server/container to the machine network target.
```

The VPN/network test does not prove that the VPN client is connected at the OS level. It proves whether FCP can reach the configured machine-network host from where the app is running.

Detailed/advanced pages should be reachable from cards on these pages, not dumped into the main menu.

## Design rule

Every main page should answer three questions:

1. What is this page for?
2. What should I do here?
3. Where does this step fit in the knowledge flow?

## In-app documentation

FCP should include a user-facing guide page inside the app. This is different from repo documentation:

- Repo docs explain design decisions for developers/researchers.
- The in-app guide explains what the operator or researcher should click next.

The in-app guide should stay short, concrete, and knowledge-flow based. It should avoid implementation details unless they help the user decide what to do next.

## Appearance

The top menu carries an appearance switch on every page, including the setup shell and the documentation portal.

```text
No stored choice = follow the operating system preference.
Stored choice    = keep light or dark until the operator switches again.
```

The choice is stored per browser and is applied before first paint, so a returning dark-mode operator never sees a light flash. Colour, elevation, radius, and control shape come from one token layer (`static/css/theme.css`); page stylesheets consume those tokens instead of literal colours, so both palettes stay in step.

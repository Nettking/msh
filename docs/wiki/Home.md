# FCP Wiki Home

This is the versioned wiki source for FCP. It mirrors the user-facing app structure so the documentation stays aligned with the UI.

## App structure

```text
Monitor   = current machine/data state and operator support
Knowledge = capture, interpret, compare, and export operator knowledge
System    = setup, sources, guide, and troubleshooting
```

## Main pages

### Monitor

- Overview
- Live
- Playback
- Assist

Use Monitor when you want to understand what is happening now.

### Knowledge

- Capture
- Review Notes
- Strategies
- Intervention Logic
- SysML Export

Use Knowledge when you want to capture operator experience and turn it into structured strategy knowledge.

### System

- Guide
- Setup
- Sources
- Diagnostics

Use System when you need setup, source configuration, documentation, or troubleshooting.

## Recommended knowledge flow

```text
raw statement
  -> review later
  -> structured strategy
  -> reusable strategy
  -> intervention logic if detectable
  -> SysML export
```

## Key pages

- [Knowledge Flow](Knowledge-Flow.md)
- [Source Setup](Source-Setup.md)
- [SysML Export](SysML-Export.md)

## Important distinction

```text
Structured strategy = interpreted operator knowledge.
Intervention logic  = technical YAML rule for detecting candidate situations from telemetry.
```

The app should not force formal modelling during a site visit. Capture first; structure later.

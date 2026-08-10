# FCP Goal Agent

This is an experimental desktop automation layer for `new-stuff`.

It is different from `desktop-agent.py`:

- `desktop-agent.py` is a specialized workflow for GitHub Desktop, repository sync, and starting FCP.
- `goal-agent.py` is a reusable goal runner. It asks what you want to do, proposes a task list, asks before starting each task, then uses screenshots, Ollama vision, clicks, keyboard actions, and user confirmations to work through the tasks.

## What it can do

The intended interaction is:

```text
What do you want me to do? look up what is on at the movies and make a PowerPoint
```

The agent should then:

1. Ask Ollama for a task list.
2. Show the proposed tasks.
3. Ask whether to start, replan, or cancel.
4. Start each task only after confirmation.
5. Take screenshots during each task.
6. Ask the vision model for the next desktop action.
7. Execute only the allowed action types.
8. Ask you to confirm when a task appears complete.
9. Save logs and screenshots under `new-stuff/goal_agent_output/`.

## Requirements

Install the same core dependencies used by the other `new-stuff` scripts:

```powershell
cd C:\wsl\fcp\new-stuff
py -m venv .venv
.\.venv\Scripts\activate
py -m pip install requests pillow pyautogui pyperclip
```

The default model settings match the current `new-stuff` experiments:

```text
Ollama URL: http://192.168.10.172:11434
Model: qwen3-vl:8b-instruct
```

Install the model on the Ollama computer:

```powershell
ollama pull qwen3-vl:8b-instruct
```

Test connectivity from the computer running the agent:

```powershell
curl http://192.168.10.172:11434/api/version
curl http://192.168.10.172:11434/api/tags
```

## Run interactively

```powershell
cd C:\wsl\fcp\new-stuff
.\.venv\Scripts\activate
py goal-agent.py
```

The program starts by asking:

```text
What do you want me to do?
```

## Run with a goal directly

```powershell
py goal-agent.py "look up what is on at the movies and make a PowerPoint"
```

## Safer test mode

Dry run still plans and asks the model what it would do, but does not physically click or type:

```powershell
py goal-agent.py --dry-run "look up what is on at the movies and make a PowerPoint"
```

Ask before every low-level desktop action:

```powershell
py goal-agent.py --confirm-each-action
```

## Environment variables

You can override the hard-coded defaults without editing the code:

```powershell
$env:FCP_GOAL_AGENT_OLLAMA_URL="http://192.168.10.172:11434"
$env:FCP_GOAL_AGENT_MODEL="qwen3-vl:8b-instruct"
$env:FCP_GOAL_AGENT_DRY_RUN="1"
$env:FCP_GOAL_AGENT_CONFIRM_EACH_ACTION="1"
$env:FCP_GOAL_AGENT_MAX_ACTIONS_PER_TASK="20"
```

## Allowed desktop actions

The executor intentionally exposes only a narrow action API:

- `open_url`
- `launch_app`
- `hotkey`
- `press`
- `type_text`
- `paste_text`
- `click`
- `double_click`
- `wait`
- `observe`
- `none`

The model cannot run arbitrary Python code, shell commands, or file deletion commands through this library.

## Safety behavior

The agent is intentionally conservative:

- It asks before starting the full plan.
- It asks before each task by default.
- It asks you to confirm completion of each task.
- It refuses or pauses for sensitive actions unless explicitly allowed.
- It should not enter passwords, payment details, two-factor codes, or secrets.
- It should not click buy, send, publish, delete, install, accept legal terms, or change account/security settings automatically.
- Moving the mouse to the upper-left corner triggers PyAutoGUI's emergency stop.

## Current limitations

This is still vision-driven desktop automation, not a browser DOM automation framework. It can be useful, but it will be fragile when:

- windows move or overlap,
- the browser layout changes,
- the model misreads a button,
- PowerPoint starts with unexpected templates or dialogs,
- websites require login, cookies, CAPTCHA, location selection, or payment.

For important work, run with `--confirm-each-action` first.

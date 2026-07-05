# One-command setup

Start MSH with one command:

```bash
docker compose up -d --build
```

Then open `/startup` in the browser.

The default one-command startup launches the Flask web UI and the local Ollama service. It does not start the recorder profile automatically, because that could begin polling real machines before sources are checked. Use the command-driven alternative below when the recorder service should be started as part of the same command.

The startup page is the normal place to choose or change local settings:

- server role.
- local AI model profile.
- recorder source settings.
- whether to install or update the selected Ollama model.
- continue-vs-clean behavior when old workflow state exists.

Browser setup is saved under:

```text
data/server_setup/server_settings.json
```

The file is local runtime configuration. It is under `data/`, so it persists across container rebuilds and is not committed.

## Standard AI choices

| Choice | Model | Device target |
| --- | --- | --- |
| Edge small | `smollm2:360m` | Small CPU or low memory testing. |
| Laptop standard | `llama3.2:3b` | Normal laptop or small server. |
| Workstation strong | `qwen2.5:7b` | Gaming laptop, workstation, or GPU server. |

## Command-driven alternative

Use command-driven setup when the settings should be reproducible without browser interaction, or when recorder/full-server services should be activated from the command:

```bash
python setup_msh.py --mode web-workbench --ai-profile laptop-standard --start
python setup_msh.py --mode web-ui-only --no-ai --start
```

For recorder machines, also pass recorder sources:

```bash
python setup_msh.py --mode full-server --ai-profile workstation-strong --recorder-sources "MACHINE=http://host:port/current" --start
```

Available modes:

```text
full-server
web-workbench
web-ui-only
recorder-only
prep-only
observer-sync-only
```

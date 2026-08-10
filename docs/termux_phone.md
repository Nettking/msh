# Run FCP on an Android phone with Termux

This profile runs FCP on Android without Docker and without a model server on the phone. It uses Termux `proot-distro` to build the existing FCP Dockerfile into a rootless Linux container, so the Python dependencies run in a normal Linux userland instead of directly against Android's Python ABI.

The phone profile provides:

- the Flask workbench;
- background orchestration;
- example JSONL telemetry and playback;
- analysis and control pages;
- operator statement capture, review, and structuring;
- strategy comparison and intervention logic;
- SysML export;
- Parquet/DuckDB cache rebuilding;
- optional MTConnect recording when a reachable source is provided;
- optional Observer Phoenix synchronization when credentials and connectivity are configured.

Local Ollama is deliberately disabled. The AI explainer can instead use Ollama contributed by a connected laptop or workstation; see [Connected capabilities](connected_capabilities.md).

## Requirements

- A current Termux installation.
- Several GB of free storage for the Linux image and Python packages.
- Git access to this repository.
- Android battery optimization disabled for Termux during long builds when needed.

## First installation

Using SSH:

```bash
cd ~
git clone git@github.com:this repository.git
cd fcp
bash termux/setup-phone.sh
```

Using HTTPS:

```bash
cd ~
git clone <repository-url> fcp
cd fcp
bash termux/setup-phone.sh
```

The setup script:

1. installs `proot-distro`, Git, OpenSSH, and curl in Termux;
2. builds the root-level `Dockerfile` without a Docker daemon;
3. installs the resulting container as `fcp-phone`;
4. creates persistent data and result directories under `~/fcp-phone-state`;
5. writes safe web-workbench defaults with AI disabled, while leaving the browser setup pending;
6. copies bundled example data into `data/demo` when no JSONL telemetry exists.

The first build downloads an Ubuntu/Python container base and all Python dependencies. It can therefore take a while.

## Start and open FCP

```bash
bash termux/fcp-phone.sh start
bash termux/fcp-phone.sh open
```

On a fresh phone installation, the browser opens the focused setup wizard. The technical defaults written by the Termux installer do not count as a completed user setup. After setup and the session-start choice, a compact first-task screen lets the user capture an operator statement, connect machine data, or open the full workbench.

Alternatively, open this manually in the Android browser:

```text
http://127.0.0.1:5000
```

The server continues in the background after the start command returns.

## Common commands

```bash
bash termux/fcp-phone.sh doctor
bash termux/fcp-phone.sh status
bash termux/fcp-phone.sh logs
bash termux/fcp-phone.sh restart
bash termux/fcp-phone.sh stop
```

Run in the foreground when debugging:

```bash
bash termux/fcp-phone.sh foreground
```

Open a shell inside the Linux container:

```bash
bash termux/fcp-phone.sh shell
```

## Demo data and local features

Restore the bundled example data:

```bash
bash termux/fcp-phone.sh demo-reset
```

Rebuild the analytical Parquet/DuckDB cache:

```bash
bash termux/fcp-phone.sh cache-rebuild
```

Run the one-shot orchestration CLI:

```bash
bash termux/fcp-phone.sh prep
```

The normal web-workbench already starts background orchestration. The explicit prep command is mainly useful for troubleshooting or testing the CLI path.

## MTConnect recorder

The recorder requires a real MTConnect `/current` endpoint reachable from the phone. Run it in the foreground with one or more semicolon-separated sources:

```bash
bash termux/fcp-phone.sh recorder 'IG500=http://192.168.200.251:5000/current'
```

Multiple sources:

```bash
bash termux/fcp-phone.sh recorder 'IG500=http://host-a:5000/current;VTC=http://host-b:5000/current'
```

Stop it with `Ctrl+C`. Recorded data is stored in the persistent phone data directory.

## Observer Phoenix synchronization

After configuring the Observer Phoenix source and credentials through the FCP source pages, run one synchronization with:

```bash
bash termux/fcp-phone.sh observer-sync
```

This function still requires valid credentials, network/VPN access, and a reachable external system.

## Persistent files

The Linux container can be rebuilt without deleting normal FCP data because these host paths are bind-mounted into it:

```text
~/fcp-phone-state/data
~/fcp-phone-state/results
```

The server log is:

```text
~/fcp-phone-state/results/termux-phone.log
```

## Update FCP

```bash
cd ~/fcp
bash termux/fcp-phone.sh update
```

Normal updates pull the latest `main`, reuse the installed Linux/Python environment, and restart FCP automatically when it was already running. The application checkout is mounted into the container, so ordinary Python, template, CSS, JavaScript, and documentation changes do not reinstall dependencies.

The updater fingerprints `Dockerfile` and `requirements.txt`. It performs the slower clean rebuild only when either file changes, the installed Python environment fails validation, or no container exists. Force that recovery path explicitly with:

```bash
bash termux/fcp-phone.sh rebuild
```

External `data` and `results` directories remain preserved in both paths. Custom browser setup is also preserved, including a connected laptop model provider. Untouched defaults created by an older phone installer are migrated once to pending first-time setup; custom roles, AI providers, and recorder settings are not reset.

## Scope and limitations

The phone profile is intended for testing, demonstrations, field-note capture, and development. It is not the recommended always-on production deployment.

Most local FCP functions can be exercised with the bundled example data. Functions that depend on external systems cannot be simulated merely by installing the app:

- MTConnect tests and recording require a reachable MTConnect endpoint;
- VPN/network tests require an actual target network;
- Observer Phoenix synchronization requires credentials and connectivity;
- manufacturing conclusions require real telemetry and validation;
- the AI page requires a reachable connected Ollama provider because this profile intentionally does not install a model server on the phone.

Do not expose port 5000 directly to the public internet. Keep the phone server on localhost, a trusted LAN, or a trusted VPN.

# Run MSH on an Android phone with Termux

This profile runs MSH on Android without Docker and without a model server on the phone. It uses Termux `proot-distro` to build the existing MSH Dockerfile into a rootless Linux container, so the Python dependencies run in a normal Linux userland instead of directly against Android's Python ABI.

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
git clone git@github.com:Nettking/msh.git
cd msh
bash termux/setup-phone.sh
```

Using HTTPS:

```bash
cd ~
git clone https://github.com/Nettking/msh.git
cd msh
bash termux/setup-phone.sh
```

The setup script:

1. installs `proot-distro`, Git, OpenSSH, and curl in Termux;
2. builds the root-level `Dockerfile` without a Docker daemon;
3. installs the resulting container as `msh-phone`;
4. creates persistent data and result directories under `~/msh-phone-state`;
5. writes safe web-workbench defaults with AI disabled, while leaving the browser setup pending;
6. copies bundled example data into `data/demo` when no JSONL telemetry exists.

The first build downloads an Ubuntu/Python container base and all Python dependencies. It can therefore take a while.

## Start and open MSH

```bash
bash termux/msh-phone.sh start
bash termux/msh-phone.sh open
```

On a fresh phone installation, the browser opens the focused setup wizard. The technical defaults written by the Termux installer do not count as a completed user setup.

Alternatively, open this manually in the Android browser:

```text
http://127.0.0.1:5000
```

The server continues in the background after the start command returns.

## Common commands

```bash
bash termux/msh-phone.sh doctor
bash termux/msh-phone.sh status
bash termux/msh-phone.sh logs
bash termux/msh-phone.sh restart
bash termux/msh-phone.sh stop
```

Run in the foreground when debugging:

```bash
bash termux/msh-phone.sh foreground
```

Open a shell inside the Linux container:

```bash
bash termux/msh-phone.sh shell
```

## Demo data and local features

Restore the bundled example data:

```bash
bash termux/msh-phone.sh demo-reset
```

Rebuild the analytical Parquet/DuckDB cache:

```bash
bash termux/msh-phone.sh cache-rebuild
```

Run the one-shot orchestration CLI:

```bash
bash termux/msh-phone.sh prep
```

The normal web-workbench already starts background orchestration. The explicit prep command is mainly useful for troubleshooting or testing the CLI path.

## MTConnect recorder

The recorder requires a real MTConnect `/current` endpoint reachable from the phone. Run it in the foreground with one or more semicolon-separated sources:

```bash
bash termux/msh-phone.sh recorder 'IG500=http://192.168.200.251:5000/current'
```

Multiple sources:

```bash
bash termux/msh-phone.sh recorder 'IG500=http://host-a:5000/current;VTC=http://host-b:5000/current'
```

Stop it with `Ctrl+C`. Recorded data is stored in the persistent phone data directory.

## Observer Phoenix synchronization

After configuring the Observer Phoenix source and credentials through the MSH source pages, run one synchronization with:

```bash
bash termux/msh-phone.sh observer-sync
```

This function still requires valid credentials, network/VPN access, and a reachable external system.

## Persistent files

The Linux container can be rebuilt without deleting normal MSH data because these host paths are bind-mounted into it:

```text
~/msh-phone-state/data
~/msh-phone-state/results
```

The server log is:

```text
~/msh-phone-state/results/termux-phone.log
```

## Update MSH

```bash
cd ~/msh
bash termux/msh-phone.sh update
```

This pulls the latest `main`, rebuilds the Linux container, and preserves the external `data` and `results` directories. Custom browser setup is preserved, including a connected laptop model provider. Untouched defaults created by an older phone installer are migrated once to pending first-time setup; custom roles, AI providers, and recorder settings are not reset.

## Scope and limitations

The phone profile is intended for testing, demonstrations, field-note capture, and development. It is not the recommended always-on production deployment.

Most local MSH functions can be exercised with the bundled example data. Functions that depend on external systems cannot be simulated merely by installing the app:

- MTConnect tests and recording require a reachable MTConnect endpoint;
- VPN/network tests require an actual target network;
- Observer Phoenix synchronization requires credentials and connectivity;
- manufacturing conclusions require real telemetry and validation;
- the AI page requires a reachable connected Ollama provider because this profile intentionally does not install a model server on the phone.

Do not expose port 5000 directly to the public internet. Keep the phone server on localhost, a trusted LAN, or a trusted VPN.

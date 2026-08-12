# Run FCP on an Android phone with Termux

Status: **current user guide**
Reviewed: **2026-08-12**

This profile runs FCP on Android without Docker and without a model server on the phone. It uses Termux `proot-distro` so the Python dependencies run in a normal Linux userland.

Local Ollama is deliberately disabled. The AI explainer can use a trusted connected Ollama provider instead; see [Connected capabilities](connected_capabilities.md).

## Requirements

- A current Termux installation.
- Several GB of free storage.
- Git access to the repository.
- Android battery optimization disabled for Termux during long builds when needed.

## First installation

```bash
cd ~
git clone <repository-url> fcp
cd fcp
bash termux/setup-phone.sh
```

The installer creates persistent state under `~/fcp-phone-state`, installs the PRoot environment, and leaves browser setup pending.

## Start and open FCP

```bash
bash termux/fcp-phone.sh start
bash termux/fcp-phone.sh open
```

The phone launcher starts the FCP web runtime and one bounded, single-instance Federation update agent.

### First human account on a standalone/local phone authority

If this phone is acting as a fresh local authority and has zero human users, opening FCP redirects to `/admin/users/bootstrap`. Create the first active administrator there with a valid email and a confirmed password of at least 12 characters, sign in, then continue device/Federation onboarding.

### Phone already paired as a Federation member

A remotely paired member with no local shadow human users does not claim local first-admin authority. Use Federation human sign-in instead.

## Common commands

```bash
bash termux/fcp-phone.sh doctor
bash termux/fcp-phone.sh status
bash termux/fcp-phone.sh logs
bash termux/fcp-phone.sh restart
bash termux/fcp-phone.sh stop
```

`status` reports the phone Federation update-agent state as well.

Foreground debugging:

```bash
bash termux/fcp-phone.sh foreground
```

Shell inside PRoot:

```bash
bash termux/fcp-phone.sh shell
```

## Persistent files

Normal state survives PRoot rebuilds through:

```text
~/fcp-phone-state/data
~/fcp-phone-state/results
```

Update-agent log:

```text
~/fcp-phone-state/results/termux-update-agent.log
```

## Federation updates

A phone that already has the phone update agent can participate in the **current operational leader's** **Check for updates** / **Update all devices** flow while online.

The phone independently validates the exact approved `main` target, fast-forwards only, updates/rebuilds the PRoot environment when required, restarts FCP, and reports success only after the requested commit is healthy.

An older phone installation from before this capability needs one bootstrap update/restart:

```bash
cd ~/fcp
bash termux/fcp-phone.sh update
bash termux/fcp-phone.sh start
```

After that, future approved Federation updates do not require a separate manual Termux update command.

Manual update remains available:

```bash
bash termux/fcp-phone.sh update
```

Force a clean environment rebuild only when needed:

```bash
bash termux/fcp-phone.sh rebuild
```

The external `data` and `results` directories remain preserved.

## MTConnect recorder

A phone can run MTConnect recording when the endpoint is reachable:

```bash
bash termux/fcp-phone.sh recorder 'IG500=http://192.168.200.251:5000/current'
```

Multiple sources can be separated with semicolons.

## Observer Phoenix synchronization

After configuring the source/credentials in FCP:

```bash
bash termux/fcp-phone.sh observer-sync
```

This requires real credentials and network/VPN access.

## Scope and limitations

The phone profile is intended for development, demonstrations, field-note capture, and testing. It is not the recommended always-on production deployment.

Functions depending on real external systems still require them: MTConnect endpoints, VPN/network targets, Observer credentials, and a connected AI provider.

Do not expose port 5000 directly to the public internet. Keep it on localhost or a trusted private network/VPN.

## Related guides

- [Quick start](quick_start.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Connected capabilities](connected_capabilities.md)

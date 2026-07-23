# Connected capabilities

MSH can keep the web workbench on one device and use a capability contributed by another device. The first supported shared capability is an Ollama language-model provider.

The intended phone setup is:

```text
Android phone                         Laptop
MSH webapp + repository context  ->  Ollama API + language model
http://127.0.0.1:5000                http://<laptop-ip>:11434
```

The MSH device sends the repository question and retrieved repository context to the selected provider. Raw telemetry remains excluded from the AI context by default.

## Install the MSH provider on the laptop

The recommended provider is installed from the MSH repository. Only Docker and Git are required on the laptop:

```bash
git clone https://github.com/Nettking/msh.git
cd msh
docker compose --profile provider run --rm model-provider-install
```

The first run pulls the Ollama container and the default `edge-small` model (`smollm2:360m`). It starts a headless MSH provider on port `11434`; it does not start a second Flask workbench. The model remains in the persistent `model_provider_models` Docker volume, so ordinary restarts and repository updates do not download it again.

An equivalent setup-helper command is:

```bash
python setup_msh.py --mode language-model-provider --ai-profile edge-small --start --pull-model
```

Verify or control the provider from the laptop:

```bash
docker compose --profile provider ps model-provider
docker compose --profile provider up -d model-provider
docker compose --profile provider down
```

Keep port `11434` limited to a trusted LAN or VPN. A normal local Ollama endpoint does not add LAN authentication.

Find the laptop's LAN IPv4 address with `ipconfig` on Windows. Test from the phone's Termux shell, replacing the example address:

```bash
curl http://192.168.1.50:11434/api/tags
```

The response should be JSON containing a `models` list.

## Connect MSH to the laptop

In MSH:

1. Open the mobile menu and select **System -> Connections**.
2. Enable the AI explainer.
3. Select **Connected computer**.
4. Name it, for example `Laptop`.
5. Enter `http://<laptop-ip>:11434`.
6. Select **Test provider connection**.
7. Continue to the model step, select **Edge small**, then save.

The connection test reads `/api/tags` from the provider and updates the model readiness shown in setup. AI Explainer then uses the saved provider URL immediately; changing provider does not require rebuilding MSH.

## Current scope

The connection record identifies:

- the contributing machine;
- the capability type (`language-model`);
- the protocol (`Ollama HTTP API`);
- the endpoint;
- the selected model profile;
- live reachability and installed-model status.

This is a deliberate first step toward multiple cooperating MSH devices. It supports one active language-model provider. Automatic discovery, load balancing, credentials, internet exposure, and a general multi-provider registry are not part of this version.

## Troubleshooting

If the MSH connection test fails:

- confirm both devices are on the same trusted LAN or VPN;
- confirm the URL uses the laptop's address, not `localhost` or `127.0.0.1`;
- run `curl http://<laptop-ip>:11434/api/tags` from the MSH device;
- run `docker compose --profile provider ps model-provider` on the laptop;
- check the laptop's private-network firewall rule for TCP `11434`;
- avoid guest Wi-Fi/client-isolation networks, which prevent devices from reaching each other.

# Connected capabilities

MSH can keep the web workbench on one device and use a capability contributed by another device. The first supported shared capability is an Ollama language-model provider.

The intended phone setup is:

```text
Android phone                         Laptop
MSH webapp + repository context  ->  Ollama API + language model
http://127.0.0.1:5000                http://<laptop-ip>:11434
```

The MSH device sends the repository question and retrieved repository context to the selected provider. Raw telemetry remains excluded from the AI context by default.

## Prepare Ollama on the laptop

Ollama binds to `127.0.0.1:11434` by default. To let another trusted device connect, set:

```text
OLLAMA_HOST=0.0.0.0:11434
```

On Windows:

1. Quit Ollama from the taskbar.
2. Open **Edit environment variables for your account**.
3. Add or edit `OLLAMA_HOST` with the value `0.0.0.0:11434`.
4. Start Ollama again from the Start menu.
5. If Windows asks, allow Ollama on private networks. Otherwise add a private-network inbound firewall rule for TCP port `11434`.

These environment-variable steps follow the [official Ollama FAQ](https://docs.ollama.com/faq). Keep the port limited to a trusted LAN or VPN; a normal local Ollama endpoint does not add LAN authentication.

Install a model on the laptop, for example:

```bash
ollama pull qwen2.5:7b
```

Find the laptop's LAN IPv4 address with `ipconfig` on Windows. Test from the phone's Termux shell, replacing the example address:

```bash
curl http://192.168.1.50:11434/api/tags
```

The response should be JSON containing a `models` list.

## Connect MSH to the laptop

In MSH:

1. Open **System -> Setup**.
2. Choose **Change setup**, then open the **AI** step.
3. Enable the AI explainer.
4. Select **Connected computer**.
5. Name it, for example `Laptop`.
6. Enter `http://<laptop-ip>:11434`.
7. Select **Test provider connection**.
8. Select a model that is installed on the laptop, then save.

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
- restart Ollama after changing `OLLAMA_HOST`;
- check the laptop's private-network firewall rule for TCP `11434`;
- avoid guest Wi-Fi/client-isolation networks, which prevent devices from reaching each other.

# Connected capabilities

FCP can keep the web workbench on one device and use a capability contributed by another device. The first supported shared capability is an Ollama language-model provider. Phase F7 adds a logical runtime that can keep several trusted language-model providers registered in the same session and select per request by explicit model, modality, availability, exclusion, and capacity policy.

The common phone setup remains:

```text
Android phone                         Laptop
FCP webapp + repository context  ->  Ollama API + language model
http://127.0.0.1:5000                http://<laptop-ip>:11434
```

The FCP device sends the repository question and retrieved repository context to the selected provider. Raw telemetry remains excluded from the AI context by default.

## Install the FCP provider on the laptop

The recommended provider is installed from the FCP repository. Only Docker and Git are required on the laptop:

```bash
git clone <repository-url> fcp
cd fcp
docker compose --profile provider run --rm model-provider-install
```

The first run pulls the Ollama container and the default `edge-small` model (`smollm2:360m`). It starts a headless FCP provider on port `11434`; it does not start a second Flask workbench. The model remains in the persistent `model_provider_models` Docker volume, so ordinary restarts and repository updates do not download it again.

An equivalent setup-helper command is:

```bash
python setup_fcp.py --mode language-model-provider --ai-profile edge-small --start --pull-model
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

## Connect FCP to the laptop

In FCP:

1. Open the mobile menu and select **System -> Connections**.
2. Enable the AI explainer.
3. Select **Connected computer**.
4. Name it, for example `Laptop`.
5. Enter `http://<laptop-ip>:11434`.
6. Select **Test provider connection**.
7. Continue to the model step, select **Edge small**, then save.

The connection test reads `/api/tags` from the provider and updates the model readiness shown in setup. AI Explainer then uses the saved provider immediately; changing the configured connection does not require rebuilding FCP.

## Logical provider runtime

The setup UI persists one configured local or connected Ollama endpoint. F7.7 wraps that endpoint as a logical `language-model` provider and may retain it together with additional trusted, session-bound providers registered by application integration.

For each request, the runtime:

- validates the request and provider protocol versions;
- requires the same logical session;
- translates model and modality needs into explicit capability requirements;
- filters unavailable, incompatible, excluded, or over-capacity providers;
- selects deterministically rather than using primary/replica semantics;
- enforces a bounded pending queue and provider concurrency limit;
- passes timeout and cancellation only to adapters that declare support;
- permits fallback only for explicitly allowlisted transient, timeout, or overload failures;
- returns structured provider attempts, selection reasoning, results, and errors.

The configured Ollama connection keeps one logical provider identity, scheduler, and capacity counter across model changes. Switching requested model cannot create a parallel runtime that bypasses the provider concurrency limit.

The AI page and JSON response may show a safe provider label, logical capability ID, supported models and modalities, capacity, queue depth, and the selection reason. They do not expose the configured base URL, IP address, port, credentials, or backend path.

## Authority boundary

A language-model provider is an inference capability only. Registration or selection does not grant storage read, storage write, leadership, database access, artifact access, session administration, or permission to execute shell commands.

Repository context supplied to AI remains selected by the FCP application. Protected job inputs and artifacts require their own short-lived, job-scoped authorization. Cross-session provider registration and cross-session requests fail closed.

Ollama must remain private. Do not publish port `11434` to the internet or treat provider status as proof that the endpoint is authenticated. Use a trusted LAN, VPN, or a separately approved authenticated transport.

## Current limitations

The setup interface still configures one Ollama connection at a time. Additional simultaneous providers require trusted application-side registration; automatic discovery and enrollment from the federated control plane are not yet part of this setup flow.

The F7.7 AI queue is bounded but process-local rather than a durable distributed interactive request queue. Production cost, latency, locality, fairness, quotas, model warm pools, streaming responses, untrusted-provider sandboxing, internet deployment, marketplace behavior, and payment are outside the current implementation.

## Troubleshooting

If the FCP connection test fails:

- confirm both devices are on the same trusted LAN or VPN;
- confirm the URL uses the laptop's address, not `localhost` or `127.0.0.1`;
- run `curl http://<laptop-ip>:11434/api/tags` from the FCP device;
- run `docker compose --profile provider ps model-provider` on the laptop;
- check the laptop's private-network firewall rule for TCP `11434`;
- avoid guest Wi-Fi/client-isolation networks, which prevent devices from reaching each other.

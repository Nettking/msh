# Getting started with FCP

Status: **current user guide**
Reviewed: **2026-08-07**

This guide gives you the mental model you need before using FCP. If you only want to install and start the software, go directly to the [Quick start](quick_start.md).

## What FCP is

FCP is a workbench for collecting, understanding, and sharing machine-related capabilities across trusted devices.

One FCP installation represents one persistent device. A device can use the workbench and can also contribute selected capabilities such as recording, a language model, registered compute handlers, or storage capacity. Those capabilities are separate choices; a device does not receive one permanent product role.

## The five ideas to understand first

### 1. A device has a persistent identity

FCP treats each installation as a device with its own identity. Normal restarts and updates reuse that identity. Starting fresh is an explicit action because replacing identity also changes Federation membership and trust state.

### 2. Devices meet inside a Federation

A Federation is the trusted boundary in which FCP devices discover and use approved capabilities. Discovery alone is not trust: another machine does not gain authority merely because it can be seen on the network.

### 3. Inspection describes what a device can do

During first-time setup, FCP inspects the local device and stores capability evidence. Inspection answers questions such as whether a recorder, model provider, compute handler, or storage candidate is available.

Inspection is evidence, not permission. It does not automatically make a capability available to other devices.

### 4. Contribution is an explicit choice

A device owner can choose which eligible capabilities to contribute. FCP keeps contribution intent separate from authority so that benchmarking, AI output, or hardware discovery cannot silently grant access.

### 5. The workbench is where you use the system

After onboarding, the normal FCP interface gives you access to Federation status, data sources, recording, workflows, knowledge capture, AI explanation, playback, and generated analyses.

## Your first ten minutes

Use this path for a new installation:

1. Follow the [Quick start](quick_start.md) to start FCP.
2. Open `/onboarding` and create or load the device identity.
3. Join, reconnect to, or create a Federation.
4. Run **Inspect** so FCP records the device's local capability evidence.
5. Finish setup and open **Federation**.
6. Review the device and capability cards before enabling any optional contribution.
7. Open the workbench feature that matches what you want to do next.

The mandatory first-run flow is deliberately short:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

Benchmarks and contribution choices are follow-up actions. They do not need to be completed before using the normal workbench.

## Where to go next

### I want to operate FCP

Read the [Operator guide](operator_guide.md) for the normal product surfaces, including Federation, Monitor, Knowledge, System, sources, recording, benchmarks, and contributions.

### I want to connect another device

Read [Connected capabilities](connected_capabilities.md) to understand how trusted devices expose usable capabilities to each other.

### I want to configure data collection

Use the [Operator guide](operator_guide.md) together with [Source synchronization](source_synchronization.md) and the [Data contract](data_contract.md).

### I want to understand how the system works

Read [Current architecture](architecture.md) for the component and data-flow view, then [Federated network reference](federated_session_network.md) for identity, membership, transport, storage, capabilities, and failure behavior.

### Something is not working

Start with [Troubleshooting](troubleshooting.md). For network exposure, recorder configuration, model installation, and deployment administration, use [Server setup](server_setup.md).

## Important authority boundaries

FCP intentionally separates evidence from authority:

- discovery is not trust;
- inspection and benchmarks are evidence, not activation;
- contribution intent is not authority;
- AI may explain or propose but does not approve, assign authority, or execute unregistered code;
- storage candidates cannot assign themselves primary or replica authority;
- compute is limited to explicitly registered handlers.

These boundaries are part of the product model, not optional security recommendations.

## Development material versus user documentation

The `docs/implementation/` tree contains active plans, acceptance material, reference material, and retained history. It is useful when developing FCP, but it is not the normal starting point for operating the product.

For normal use, prefer the current user guides linked from the [documentation home](index.md).
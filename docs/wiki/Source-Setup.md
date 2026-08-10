# Source Setup

System -> Sources is where FCP stores machine and source configuration.

## What belongs here

- machines
- MTConnect adapter URLs
- VPN/network reachability targets
- vibration sensors
- Observer Phoenix connector access
- machine/source notes

The inventory is stored locally under:

```text
data/source_config/machines_and_sensors.json
```

## Machine fields

A machine can include:

- name
- machine type
- controller / adapter
- MTConnect URL
- VPN/network test host
- VPN/network test port
- notes

## MTConnect test

The Test MTConnect button checks the configured MTConnect endpoint from the Flask server/container.

If the URL is a base adapter URL, FCP tests `/current` automatically.

Example:

```text
10.0.0.20:5000
-> http://10.0.0.20:5000/current
```

A successful test means FCP reached the endpoint and received a response.

## VPN/network test

The Test VPN/network button opens a TCP connection from the Flask server/container to the configured host and port.

This does not prove that the VPN client is connected at the operating-system level. It proves the operationally useful thing for FCP: whether the app can reach the configured machine-network target from where it is running.

If no VPN/network test host is configured, FCP falls back to the MTConnect host and port when possible.

## Vibration sensors

A vibration sensor can include:

- sensor name
- assigned machine
- source system
- signal/channel
- axis
- unit
- sampling rate
- enabled/disabled state
- notes

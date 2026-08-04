# Edenic Local Broker

Use a Bluelab Guardian Monitor Wi-Fi entirely offline. The meter keeps
publishing exactly as it always did — it just talks to your Home Assistant
box instead of Edenic's servers.

**This add-on is a complete MQTT broker. It is the only broker you need.**

    Guardian  ──(1883)──▶  Edenic Local Broker  ◀──(1883)──  Home Assistant

## Why it exists

The Guardian publishes plaintext MQTT on port 1883, once a minute:

```json
{"ts":1785817606767,"values":{"electrical_conductivity":1.42,"ph":6.36,"temperature":24.87}}
```

It connects with a UUID username and an **empty password**. The official
Mosquitto add-on rejects that outright (`received null username or password
for unpwd check`) because anonymous logins were removed from it.

Multiple meters also all publish to the same `v1/devices/me/telemetry`
topic, with no device ID in the payload. The only thing distinguishing them
is the MQTT username — which a normal broker never exposes. This broker owns
the connection, so it can.

## Install

1. Copy the `edenic_broker` folder to `/addons` on your Home Assistant
   machine (or add this repository in the add-on store).
2. Settings → Add-ons → Add-on Store → ⋮ → **Check for updates**.
3. Install **Edenic Local Broker** and start it.
4. **Stop any other MQTT broker add-on** — only one can own port 1883, and
   the Guardian hard-codes it.
5. Settings → Devices & Services → **MQTT** → point it at this broker:
   host `localhost` (or your HA machine's IP), port `1883`, no username or
   password. It accepts anonymous connections.
6. Add a DNS rewrite pointing the Guardian's cloud hostname at your Home
   Assistant IP. In AdGuard Home: Filters → DNS rewrites. Find the hostname
   in the Query Log, filtered by the meter's IP.
7. Within a minute a "Bluelab Guardian" device appears under MQTT.

## What you get

Key names taken from Edenic's own REST API (`api.edenic.io`).

**Sensors** — `ph`, `electrical_conductivity`, `temperature`

**Binary sensors** (from `alarm.*`, evaluated on the meter itself) —
pH high/low, EC high/low, temperature high/low, `calibration_required`

**Config entities** (from `setting.*`, only when `allow_writes` is on) —
the six thresholds as numbers, plus an Alarms Enabled switch

## Migrating from Mosquitto

Other MQTT devices can use this broker too — it is a normal broker with
wildcard subscriptions and retained messages. But it has **no
authentication**: any client on the network can connect. Keep it on a
trusted LAN or an IoT VLAN. If you need passwords or TLS, run Mosquitto on
a different port and bridge instead.

Retained messages are held in memory only and are lost on restart. Home
Assistant re-requests discovery on reconnect, so entities come back on their
own once the meter publishes again.

## About allow_writes

Off by default. Reading is passive; writing is not.

When you change a threshold, the add-on pushes the **entire** `setting.*`
block to the meter in one message — all six thresholds plus
`setting.alarms` — because that is what Edenic's cloud does. Sending a
single key has never been observed and may not work.

Two caveats:

1. The downstream topic (`v1/devices/me/attributes`) is inferred from the
   platform's conventions, not confirmed against the firmware. If the meter
   ignores it, writes silently do nothing.
2. The whole block is sent together, so the add-on must know your current
   thresholds first. Let the meter report its attributes at least once
   after startup before changing anything, or you may push zeros.

If in doubt, leave writes off and set thresholds with the meter's own
buttons. Alarm *reporting* works either way.

## Troubleshooting

Set `log_level: debug` and watch the add-on's Log tab. You want:

```
Broker listening on 0.0.0.0:1883 (writes disabled)
Connected ('10.4.0.183', ...) as ESP32_xxxxxx (MQTT v4 keepalive=45s user=<uuid>)
Announced <uuid> to Home Assistant
Telemetry <uuid>: {...}
```

- No `Connected` line from the meter → the DNS rewrite isn't taking effect.
  Check AdGuard's Query Log for the meter's IP.
- `Connected` but no `Telemetry` → wait a full minute; it publishes on its
  own schedule.
- Entities missing in HA → confirm the MQTT integration is pointed at *this*
  broker, and that discovery is enabled in its options.

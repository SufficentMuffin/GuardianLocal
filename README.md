# Edenic Local Broker

Use a Bluelab Guardian Monitor Wi-Fi entirely offline. The meter keeps
publishing exactly as it always did — it just talks to your Home Assistant
box instead of Edenic's servers.

For the user, the whole setup is: install the add-on, add one DNS rewrite,
done. The sensors appear by themselves.

## Why this exists

The Guardian publishes plaintext MQTT on port 1883, once a minute:

```json
{"ts":1785817606767,"values":{"electrical_conductivity":1.42,"ph":6.36,"temperature":24.87}}
```

You cannot point it at the official Mosquitto add-on, because the device
sends a UUID username with an **empty password**, and anonymous logins were
deliberately removed from that add-on. Mosquitto rejects it at CONNECT with
`received null username or password for unpwd check`.

You also cannot easily run several meters through a normal broker: they all
publish to the same `v1/devices/me/telemetry` topic and the payload has no
device ID in it. The only thing that distinguishes them is the MQTT
username, which brokers don't expose to subscribers.

So this add-on ships a ~250 line MQTT broker that speaks only the handful of
packets the device uses, keys each meter by its username, and forwards
telemetry to your real broker as MQTT discovery messages.

## Install

1. Settings → Add-ons → Add-on Store → ⋮ → Repositories → add this repo URL.
2. Install **Edenic Local Broker** and start it.
3. Add a DNS rewrite pointing the Guardian's cloud hostname at your Home
   Assistant IP. In AdGuard Home: Filters → DNS rewrites.
   Find the hostname first in AdGuard's Query Log, filtered by the meter's IP.
4. Wait a minute. A "Bluelab Guardian" device appears under Settings →
   Devices & Services → MQTT.

## Port 1883 conflict

The device hard-codes port 1883, so this add-on must own it. If you already
run the Mosquitto add-on on the same machine, only one can bind it. Options:

- Move Mosquitto to another port and reconfigure your other MQTT devices
  (they're configurable; the Guardian isn't), or
- Point the DNS rewrite at a different machine on your LAN and run this
  broker there instead, or
- Give the HA host a second IP and rewrite to that.

## Notes

- The meter needs NTP to validate nothing in particular, but it does use the
  clock for its `ts` field. Home Assistant timestamps on receipt, so blocking
  the device's internet access entirely is fine.
- This broker is not a general-purpose broker. Don't expose it beyond your
  LAN, and preferably keep the meter on an IoT VLAN.
- Tested against firmware reporting client ID `ESP32_*` and MQTT 3.1.1.
  If your device negotiates MQTT 5, open an issue with the add-on log.

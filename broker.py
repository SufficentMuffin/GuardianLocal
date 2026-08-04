#!/usr/bin/env python3
"""
Edenic Local Broker
-------------------
A deliberately minimal MQTT 3.1.x broker that accepts anonymous-ish
connections from Bluelab Guardian Wi-Fi / Edenic devices (which present a
UUID username and an empty password), and re-publishes their telemetry to
the real Home Assistant broker using MQTT discovery.

Why a custom broker instead of Mosquitto:
  * the HA Mosquitto add-on refuses empty passwords outright, and
  * every device publishes to the same 'v1/devices/me/telemetry' topic, so
    the only way to tell two meters apart is the MQTT username -- which a
    normal broker never exposes to subscribers.

Only what the device actually needs is implemented: CONNECT, PUBLISH,
SUBSCRIBE, PINGREQ, DISCONNECT. It is not a general purpose broker and
should not be exposed to the internet.
"""

import asyncio
import json
import logging
import os
import signal
import sys

import paho.mqtt.client as mqtt

LOG = logging.getLogger("edenic")

# --- packet types -----------------------------------------------------------
CONNECT, CONNACK, PUBLISH, PUBACK = 1, 2, 3, 4
PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK = 5, 6, 7, 8, 9
UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT = 10, 11, 12, 13, 14

# --- sensors we know how to expose ------------------------------------------
# key in payload["values"] -> (friendly name, device_class, unit, precision, icon)
SENSORS = {
    "ph": ("pH", "ph", None, 2, "mdi:ph"),
    "electrical_conductivity": ("EC", None, "mS/cm", 2, "mdi:water-opacity"),
    "temperature": ("Temperature", "temperature", "\u00b0C", 1, None),
}


class Upstream:
    """Client connection to the real Home Assistant broker."""

    def __init__(self, host, port, username, password, discovery_prefix):
        self.discovery_prefix = discovery_prefix
        self.enabled = bool(host)
        self._announced = set()
        if not self.enabled:
            LOG.warning("No upstream broker configured - telemetry will only be logged")
            return
        self._c = mqtt.Client(client_id="edenic-local-broker", clean_session=True)
        if username:
            self._c.username_pw_set(username, password)
        self._c.will_set("edenic2mqtt/bridge/state", "offline", retain=True)
        self._c.connect_async(host, port, keepalive=60)
        self._c.loop_start()
        self._c.publish("edenic2mqtt/bridge/state", "online", retain=True)
        LOG.info("Upstream broker %s:%s", host, port)

    def _announce(self, device_id):
        """Publish retained HA discovery configs, once per device."""
        if device_id in self._announced:
            return
        short = device_id.split("-")[0][:8]
        device = {
            "identifiers": [f"edenic_{device_id}"],
            "name": f"Bluelab Guardian {short}",
            "manufacturer": "Bluelab",
            "model": "Guardian Monitor Wi-Fi",
        }
        state_topic = f"edenic2mqtt/{device_id}/state"
        for key, (label, dev_class, unit, precision, icon) in SENSORS.items():
            cfg = {
                "name": label,
                "unique_id": f"edenic_{device_id}_{key}",
                "state_topic": state_topic,
                "availability_topic": f"edenic2mqtt/{device_id}/availability",
                "value_template": "{{ value_json.values.%s | default(none) }}" % key,
                "suggested_display_precision": precision,
                "device": device,
            }
            if dev_class:
                cfg["device_class"] = dev_class
                cfg["state_class"] = "measurement"
            if unit:
                cfg["unit_of_measurement"] = unit
            if icon:
                cfg["icon"] = icon
            topic = f"{self.discovery_prefix}/sensor/edenic_{device_id}/{key}/config"
            self._c.publish(topic, json.dumps(cfg), retain=True)
        self._announced.add(device_id)
        LOG.info("Announced device %s to Home Assistant", device_id)

    def telemetry(self, device_id, payload):
        if not self.enabled:
            return
        self._announce(device_id)
        self._c.publish(f"edenic2mqtt/{device_id}/state", payload, retain=True)

    def availability(self, device_id, online):
        if not self.enabled:
            return
        self._c.publish(
            f"edenic2mqtt/{device_id}/availability",
            "online" if online else "offline",
            retain=True,
        )


# --- wire format helpers ----------------------------------------------------
def encode_remaining_length(n):
    out = bytearray()
    while True:
        byte = n % 128
        n //= 128
        if n:
            byte |= 0x80
        out.append(byte)
        if not n:
            return bytes(out)


async def read_remaining_length(reader):
    multiplier, value = 1, 0
    while True:
        (byte,) = await reader.readexactly(1)
        value += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            return value
        multiplier *= 128
        if multiplier > 128 ** 3:
            raise ValueError("malformed remaining length")


def take_string(buf, pos):
    length = int.from_bytes(buf[pos:pos + 2], "big")
    pos += 2
    return buf[pos:pos + length].decode("utf-8", "replace"), pos + length


class Session:
    def __init__(self, reader, writer, upstream):
        self.reader, self.writer, self.upstream = reader, writer, upstream
        self.peer = writer.get_extra_info("peername")
        self.client_id = None
        self.device_id = None

    async def run(self):
        try:
            while True:
                header = await self.reader.readexactly(1)
                packet_type = header[0] >> 4
                flags = header[0] & 0x0F
                length = await read_remaining_length(self.reader)
                body = await self.reader.readexactly(length) if length else b""
                if not await self.dispatch(packet_type, flags, body):
                    break
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception:
            LOG.exception("Session error from %s", self.peer)
        finally:
            if self.device_id:
                self.upstream.availability(self.device_id, False)
                LOG.info("Device %s disconnected", self.device_id)
            self.writer.close()

    async def dispatch(self, packet_type, flags, body):
        if packet_type == CONNECT:
            return await self.on_connect(body)
        if packet_type == PUBLISH:
            return await self.on_publish(flags, body)
        if packet_type == SUBSCRIBE:
            return await self.on_subscribe(body)
        if packet_type == PINGREQ:
            self.writer.write(bytes([PINGRESP << 4, 0]))
            await self.writer.drain()
            return True
        if packet_type == DISCONNECT:
            return False
        if packet_type in (PUBREL,):
            return True
        LOG.debug("Ignoring packet type %s", packet_type)
        return True

    async def on_connect(self, body):
        proto, pos = take_string(body, 0)
        level = body[pos]
        connect_flags = body[pos + 1]
        keepalive = int.from_bytes(body[pos + 2:pos + 4], "big")
        pos += 4
        self.client_id, pos = take_string(body, pos)
        if connect_flags & 0x04:  # will
            _, pos = take_string(body, pos)
            _, pos = take_string(body, pos)
        username = None
        if connect_flags & 0x80:
            username, pos = take_string(body, pos)
        if connect_flags & 0x40:
            _, pos = take_string(body, pos)

        # Prefer the username: on these devices it is a stable UUID, while the
        # client id is derived from the MAC and can change across firmware.
        self.device_id = username or self.client_id
        LOG.info(
            "Connected %s as %s (proto=%s v%s keepalive=%ss user=%s)",
            self.peer, self.client_id, proto, level, keepalive, username,
        )
        if level > 4:
            LOG.warning("Client requested MQTT v5 - responding as 3.1.1 anyway")
        self.writer.write(bytes([CONNACK << 4, 2, 0, 0]))
        await self.writer.drain()
        self.upstream.availability(self.device_id, True)
        return True

    async def on_publish(self, flags, body):
        qos = (flags >> 1) & 0x03
        topic, pos = take_string(body, 0)
        packet_id = None
        if qos:
            packet_id = int.from_bytes(body[pos:pos + 2], "big")
            pos += 2
        payload = body[pos:].decode("utf-8", "replace")
        LOG.debug("PUBLISH %s -> %s", topic, payload)

        try:
            data = json.loads(payload)
        except ValueError:
            LOG.warning("Non-JSON payload on %s, forwarding raw", topic)
            data = None

        if data is not None and "values" in data:
            self.upstream.telemetry(self.device_id, payload)
            LOG.info("Telemetry from %s: %s", self.device_id, data["values"])

        if qos == 1:
            self.writer.write(bytes([PUBACK << 4, 2]) + packet_id.to_bytes(2, "big"))
            await self.writer.drain()
        elif qos == 2:
            self.writer.write(bytes([PUBREC << 4, 2]) + packet_id.to_bytes(2, "big"))
            await self.writer.drain()
        return True

    async def on_subscribe(self, body):
        packet_id = int.from_bytes(body[0:2], "big")
        pos, granted = 2, bytearray()
        while pos < len(body):
            _topic, pos = take_string(body, pos)
            pos += 1  # requested qos
            granted.append(0)  # grant QoS 0
        payload = packet_id.to_bytes(2, "big") + bytes(granted)
        self.writer.write(
            bytes([SUBACK << 4]) + encode_remaining_length(len(payload)) + payload
        )
        await self.writer.drain()
        return True


async def main():
    level = os.environ.get("LOG_LEVEL", "info").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    upstream = Upstream(
        os.environ.get("UPSTREAM_HOST", ""),
        int(os.environ.get("UPSTREAM_PORT", "1883")),
        os.environ.get("UPSTREAM_USER", ""),
        os.environ.get("UPSTREAM_PASS", ""),
        os.environ.get("DISCOVERY_PREFIX", "homeassistant"),
    )
    port = int(os.environ.get("LISTEN_PORT", "1883"))

    server = await asyncio.start_server(
        lambda r, w: Session(r, w, upstream).run(), "0.0.0.0", port
    )
    LOG.info("Listening for Guardian devices on 0.0.0.0:%s", port)

    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: stop.done() or stop.set_result(None))
        except NotImplementedError:
            pass
    async with server:
        await stop
    LOG.info("Shutting down")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)

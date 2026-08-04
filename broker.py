#!/usr/bin/env python3
"""
Edenic Local Broker
-------------------
A small but complete MQTT 3.1.1 broker for Bluelab Guardian Wi-Fi meters.

It is the ONLY broker you need. Both sides connect to it:

    Guardian  --(port 1883)-->  this  <--(port 1883)--  Home Assistant

The meter presents a UUID username with an empty password, which the
official Mosquitto add-on rejects. This accepts it, and because it owns the
connection it can see that username -- which is the only thing telling two
meters apart, since they all publish to the same topic.

Telemetry and alarms are turned into Home Assistant MQTT discovery messages
automatically, so nothing needs configuring by hand.

Namespaces, mirrored from Edenic's REST API:

    telemetry   ph, temperature, electrical_conductivity   device -> us
    alarm.*     ph/ec/temp high+low, calibration_required  device -> us
    setting.*   the same six thresholds + alarms master    us -> device

No third-party Python dependencies.
"""

import asyncio
import json
import logging
import os
import signal
import sys

LOG = logging.getLogger("edenic")

CONNECT, CONNACK, PUBLISH, PUBACK = 1, 2, 3, 4
PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK = 5, 6, 7, 8, 9
UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT = 10, 11, 12, 13, 14

TOPIC_ATTRIBUTES = "v1/devices/me/attributes"
DEVICE_TOPIC_PREFIX = "v1/devices/"

SENSORS = {
    "ph": ("pH", "ph", None, 2, "mdi:ph"),
    "electrical_conductivity": ("EC", "conductivity", "mS/cm", 2, "mdi:fence-electric"),
    "temperature": ("Temperature", "temperature", "\u00b0C", 1, "mdi:thermometer"),
}

ALARMS = {
    "ph_high_alarm": ("pH High Alarm", "mdi:alert-circle"),
    "ph_low_alarm": ("pH Low Alarm", "mdi:alert"),
    "ec_high_alarm": ("EC High Alarm", "mdi:alert-circle"),
    "ec_low_alarm": ("EC Low Alarm", "mdi:alert"),
    "temp_high_alarm": ("Temperature High Alarm", "mdi:alert-circle"),
    "temp_low_alarm": ("Temperature Low Alarm", "mdi:alert"),
    "calibration_required": ("Calibration Required", "mdi:alert-circle-check"),
}

THRESHOLDS = {
    "ph_low_alarm": ("pH Low Threshold", 0, 14, 0.1, False),
    "ph_high_alarm": ("pH High Threshold", 0, 14, 0.1, False),
    "ec_low_alarm": ("EC Low Threshold", 0, 5, 0.1, False),
    "ec_high_alarm": ("EC High Threshold", 0, 5, 0.1, False),
    "temp_low_alarm": ("Temperature Low Threshold", 0, 50, 1, True),
    "temp_high_alarm": ("Temperature High Threshold", 0, 50, 1, True),
}


def unwrap(value):
    """Edenic wraps some attribute values as {"value": x}. Flatten those."""
    if isinstance(value, dict) and "value" in value:
        return value["value"]
    return value


# --- wire format ------------------------------------------------------------
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


def build_publish(topic, payload, retain=False):
    if isinstance(payload, str):
        payload = payload.encode()
    body = len(topic).to_bytes(2, "big") + topic.encode() + payload
    header = (PUBLISH << 4) | (1 if retain else 0)
    return bytes([header]) + encode_remaining_length(len(body)) + body


def topic_matches(filt, topic):
    """MQTT wildcard matching for + and #."""
    if filt == topic:
        return True
    f, t = filt.split("/"), topic.split("/")
    for i, part in enumerate(f):
        if part == "#":
            # '#' must be last and matches the rest, but not $-topics at root
            return not (i == 0 and t and t[0].startswith("$"))
        if i >= len(t):
            return False
        if part == "+":
            if i == 0 and t[0].startswith("$"):
                return False
            continue
        if part != t[i]:
            return False
    return len(f) == len(t)


class Broker:
    """Holds every session, the subscription table and retained messages."""

    def __init__(self, allow_writes):
        self.allow_writes = allow_writes
        self.sessions = []            # all connected sessions
        self.devices = {}             # device_id -> Session (meters only)
        self.retained = {}            # topic -> payload bytes
        self.attributes = {}          # device_id -> flattened attribute dict
        self.announced = set()

    # -- routing -----------------------------------------------------------
    def publish(self, topic, payload, retain=False):
        """Deliver a message to every matching subscriber."""
        if isinstance(payload, str):
            payload = payload.encode()
        if retain:
            if payload:
                self.retained[topic] = payload
            else:
                self.retained.pop(topic, None)
        packet = build_publish(topic, payload)
        for session in list(self.sessions):
            for filt in session.subscriptions:
                if topic_matches(filt, topic):
                    session.write(packet)
                    break

    def send_retained(self, session, filt):
        for topic, payload in self.retained.items():
            if topic_matches(filt, topic):
                session.write(build_publish(topic, payload, retain=True))

    # -- Home Assistant discovery -----------------------------------------
    def announce(self, device_id):
        if device_id in self.announced:
            return
        short = device_id.split("-")[0][:8]
        device = {
            "identifiers": [f"edenic_{device_id}"],
            "name": f"Bluelab Guardian {short}",
            "manufacturer": "Bluelab",
            "model": "Guardian Monitor Wi-Fi",
        }
        base = f"edenic2mqtt/{device_id}"
        avail = f"{base}/availability"
        prefix = os.environ.get("DISCOVERY_PREFIX", "homeassistant")

        for key, (label, dev_class, unit, precision, icon) in SENSORS.items():
            cfg = {
                "name": label,
                "unique_id": f"edenic_{device_id}_{key}",
                "state_topic": f"{base}/state",
                "availability_topic": avail,
                "value_template": "{{ value_json.values.%s | default(none) }}" % key,
                "suggested_display_precision": precision,
                "state_class": "measurement",
                "icon": icon,
                "device": device,
            }
            if dev_class:
                cfg["device_class"] = dev_class
            if unit:
                cfg["unit_of_measurement"] = unit
            self.publish(
                f"{prefix}/sensor/edenic_{device_id}/{key}/config",
                json.dumps(cfg), retain=True,
            )

        for key, (label, icon) in ALARMS.items():
            template = (
                "{%% if value_json['alarm.%s'] is defined %%}"
                "{{ 'ON' if value_json['alarm.%s'] else 'OFF' }}"
                "{%% else %%}None{%% endif %%}" % (key, key)
            )
            cfg = {
                "name": label,
                "unique_id": f"edenic_{device_id}_alarm_{key}",
                "state_topic": f"{base}/attributes",
                "availability_topic": avail,
                "value_template": template,
                "device_class": "problem",
                "icon": icon,
                "device": device,
            }
            self.publish(
                f"{prefix}/binary_sensor/edenic_{device_id}/{key}/config",
                json.dumps(cfg), retain=True,
            )

        if self.allow_writes:
            for key, (label, lo, hi, step, _is_int) in THRESHOLDS.items():
                cfg = {
                    "name": label,
                    "unique_id": f"edenic_{device_id}_setting_{key}",
                    "state_topic": f"{base}/attributes",
                    "command_topic": f"{base}/set/{key}",
                    "availability_topic": avail,
                    "value_template": "{{ value_json['setting.%s'] | default(none) }}" % key,
                    "min": lo, "max": hi, "step": step,
                    "mode": "box",
                    "entity_category": "config",
                    "device": device,
                }
                self.publish(
                    f"{prefix}/number/edenic_{device_id}/{key}/config",
                    json.dumps(cfg), retain=True,
                )
            cfg = {
                "name": "Alarms Enabled",
                "unique_id": f"edenic_{device_id}_setting_alarms",
                "state_topic": f"{base}/attributes",
                "command_topic": f"{base}/set/alarms",
                "availability_topic": avail,
                "value_template": "{{ 'ON' if value_json['setting.alarms'] else 'OFF' }}",
                "entity_category": "config",
                "icon": "mdi:bell-ring",
                "device": device,
            }
            self.publish(
                f"{prefix}/switch/edenic_{device_id}/alarms/config",
                json.dumps(cfg), retain=True,
            )

        self.announced.add(device_id)
        LOG.info("Announced %s to Home Assistant (writes=%s)", device_id, self.allow_writes)

    # -- device data -------------------------------------------------------
    def on_device_message(self, session, topic, payload):
        device_id = session.device_id
        if session.is_device is False:
            session.is_device = True
        self.devices[device_id] = session
        self.announce(device_id)

        try:
            data = json.loads(payload)
        except ValueError:
            LOG.warning("Unparsed payload on %s: %r", topic, payload[:120])
            return

        if isinstance(data, dict) and "values" in data:
            self.publish(f"edenic2mqtt/{device_id}/state", payload, retain=True)
            self.publish(f"edenic2mqtt/{device_id}/availability", "online", retain=True)
            LOG.info("Telemetry %s: %s", device_id, data["values"])
        elif isinstance(data, dict) and data:
            store = self.attributes.setdefault(device_id, {})
            store.update({k: unwrap(v) for k, v in data.items()})
            self.publish(
                f"edenic2mqtt/{device_id}/attributes", json.dumps(store), retain=True
            )
            LOG.info("Attributes %s: %s", device_id, data)

    def desired_settings(self, device_id, overrides=None):
        """Edenic always sends all six thresholds plus the master together."""
        current = self.attributes.get(device_id, {})
        out = {}
        for key, (_l, _lo, _hi, _s, is_int) in THRESHOLDS.items():
            value = unwrap(current.get(f"setting.{key}", 0))
            if overrides and key in overrides:
                value = overrides[key]
            try:
                out[f"setting.{key}"] = int(float(value)) if is_int else round(float(value), 2)
            except (TypeError, ValueError):
                out[f"setting.{key}"] = 0
        enabled = unwrap(current.get("setting.alarms", True))
        if overrides and "alarms" in overrides:
            enabled = overrides["alarms"]
        out["setting.alarms"] = bool(enabled)
        return out

    def on_command(self, topic, payload):
        """A Home Assistant number/switch changed."""
        try:
            _, device_id, _, key = topic.split("/", 3)
        except ValueError:
            return
        if not self.allow_writes:
            LOG.warning("Ignoring command %s (writes disabled)", topic)
            return
        session = self.devices.get(device_id)
        if session is None:
            LOG.warning("Command for %s but the meter is offline", device_id)
            return
        value = payload.decode() if isinstance(payload, bytes) else payload
        if key == "alarms":
            overrides = {"alarms": value.upper() in ("ON", "TRUE", "1")}
        else:
            try:
                overrides = {key: float(value)}
            except ValueError:
                LOG.warning("Non-numeric command %s=%r", key, value)
                return
        block = self.desired_settings(device_id, overrides)
        LOG.info("Pushing settings to %s: %s", device_id, block)
        session.write(build_publish(TOPIC_ATTRIBUTES, json.dumps(block)))
        self.attributes.setdefault(device_id, {}).update(block)
        self.publish(
            f"edenic2mqtt/{device_id}/attributes",
            json.dumps(self.attributes[device_id]), retain=True,
        )


class Session:
    def __init__(self, reader, writer, broker):
        self.reader, self.writer, self.broker = reader, writer, broker
        self.peer = writer.get_extra_info("peername")
        self.client_id = None
        self.device_id = None
        self.is_device = False
        self.subscriptions = set()
        self.will = None

    def write(self, packet):
        try:
            self.writer.write(packet)
        except Exception:
            LOG.debug("Write failed to %s", self.peer)

    async def run(self):
        self.broker.sessions.append(self)
        try:
            while True:
                header = await self.reader.readexactly(1)
                packet_type, flags = header[0] >> 4, header[0] & 0x0F
                length = await read_remaining_length(self.reader)
                body = await self.reader.readexactly(length) if length else b""
                if not await self.dispatch(packet_type, flags, body):
                    break
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception:
            LOG.exception("Session error from %s", self.peer)
        finally:
            if self in self.broker.sessions:
                self.broker.sessions.remove(self)
            if self.is_device and self.device_id:
                self.broker.publish(
                    f"edenic2mqtt/{self.device_id}/availability", "offline", retain=True
                )
                if self.broker.devices.get(self.device_id) is self:
                    del self.broker.devices[self.device_id]
                LOG.info("Meter %s disconnected", self.device_id)
            else:
                LOG.info("Client %s disconnected", self.client_id or self.peer)
            self.writer.close()

    async def dispatch(self, packet_type, flags, body):
        if packet_type == CONNECT:
            return await self.on_connect(body)
        if packet_type == PUBLISH:
            return await self.on_publish(flags, body)
        if packet_type == SUBSCRIBE:
            return await self.on_subscribe(body)
        if packet_type == UNSUBSCRIBE:
            return await self.on_unsubscribe(body)
        if packet_type == PINGREQ:
            self.write(bytes([PINGRESP << 4, 0]))
            await self.writer.drain()
            return True
        if packet_type == DISCONNECT:
            return False
        if packet_type == PUBREL:
            return True
        return True

    async def on_connect(self, body):
        proto, pos = take_string(body, 0)
        level = body[pos]
        connect_flags = body[pos + 1]
        keepalive = int.from_bytes(body[pos + 2:pos + 4], "big")
        pos += 4
        self.client_id, pos = take_string(body, pos)
        if connect_flags & 0x04:
            _, pos = take_string(body, pos)
            _, pos = take_string(body, pos)
        username = None
        if connect_flags & 0x80:
            username, pos = take_string(body, pos)

        # For meters the username is a stable UUID. Everything else (Home
        # Assistant, MQTT Explorer) is just a normal client.
        self.device_id = username or self.client_id
        LOG.info(
            "Connected %s as %s (%s v%s keepalive=%ss user=%s)",
            self.peer, self.client_id, proto, level, keepalive, username,
        )
        self.write(bytes([CONNACK << 4, 2, 0, 0]))
        await self.writer.drain()
        return True

    async def on_publish(self, flags, body):
        qos = (flags >> 1) & 0x03
        retain = bool(flags & 0x01)
        topic, pos = take_string(body, 0)
        packet_id = None
        if qos:
            packet_id = int.from_bytes(body[pos:pos + 2], "big")
            pos += 2
        payload = body[pos:]
        LOG.debug("PUBLISH %s -> %r", topic, payload[:200])

        if topic.startswith(DEVICE_TOPIC_PREFIX):
            # A meter reporting telemetry or attributes.
            self.broker.on_device_message(self, topic, payload.decode("utf-8", "replace"))
        elif "/set/" in topic and topic.startswith("edenic2mqtt/"):
            self.broker.on_command(topic, payload)
        else:
            # Ordinary MQTT traffic - just route it.
            self.broker.publish(topic, payload, retain=retain)

        if qos == 1:
            self.write(bytes([PUBACK << 4, 2]) + packet_id.to_bytes(2, "big"))
            await self.writer.drain()
        elif qos == 2:
            self.write(bytes([PUBREC << 4, 2]) + packet_id.to_bytes(2, "big"))
            await self.writer.drain()
        return True

    async def on_subscribe(self, body):
        packet_id = int.from_bytes(body[0:2], "big")
        pos, granted, filters = 2, bytearray(), []
        while pos < len(body):
            filt, pos = take_string(body, pos)
            pos += 1  # requested QoS
            self.subscriptions.add(filt)
            filters.append(filt)
            granted.append(0)  # we deliver at QoS 0
        payload = packet_id.to_bytes(2, "big") + bytes(granted)
        self.write(bytes([SUBACK << 4]) + encode_remaining_length(len(payload)) + payload)
        await self.writer.drain()
        for filt in filters:
            LOG.info("%s subscribed to %s", self.client_id, filt)
            self.broker.send_retained(self, filt)
        await self.writer.drain()
        return True

    async def on_unsubscribe(self, body):
        packet_id = int.from_bytes(body[0:2], "big")
        pos = 2
        while pos < len(body):
            filt, pos = take_string(body, pos)
            self.subscriptions.discard(filt)
        self.write(bytes([UNSUBACK << 4, 2]) + packet_id.to_bytes(2, "big"))
        await self.writer.drain()
        return True


async def main():
    level = os.environ.get("LOG_LEVEL", "info").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    allow_writes = os.environ.get("ALLOW_WRITES", "false").lower() == "true"
    broker = Broker(allow_writes)
    port = int(os.environ.get("LISTEN_PORT", "1883"))

    server = await asyncio.start_server(
        lambda r, w: Session(r, w, broker).run(), "0.0.0.0", port
    )
    LOG.info(
        "Broker listening on 0.0.0.0:%s (writes %s)",
        port, "enabled" if allow_writes else "disabled",
    )
    LOG.info("Point both the Guardian and the MQTT integration here.")

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

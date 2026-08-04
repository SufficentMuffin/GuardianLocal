#!/usr/bin/with-contenv bashio
# This add-on IS the MQTT broker. It needs no upstream connection.
export LISTEN_PORT=1883
export DISCOVERY_PREFIX="$(bashio::config 'discovery_prefix')"
export LOG_LEVEL="$(bashio::config 'log_level')"
export ALLOW_WRITES="$(bashio::config 'allow_writes')"

exec python3 /broker.py

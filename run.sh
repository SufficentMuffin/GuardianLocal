#!/usr/bin/with-contenv bashio
# Pull the real broker's details from the Supervisor's MQTT service so the
# user never has to type a host or password. Requires services: mqtt:need.

if bashio::services.available "mqtt"; then
    export UPSTREAM_HOST="$(bashio::services mqtt "host")"
    export UPSTREAM_PORT="$(bashio::services mqtt "port")"
    export UPSTREAM_USER="$(bashio::services mqtt "username")"
    export UPSTREAM_PASS="$(bashio::services mqtt "password")"
    bashio::log.info "Using Home Assistant broker at ${UPSTREAM_HOST}:${UPSTREAM_PORT}"
else
    bashio::log.error "No MQTT service found. Install and start a broker add-on,"
    bashio::log.error "then set up the MQTT integration, then restart this add-on."
fi

export LISTEN_PORT=1883
export DISCOVERY_PREFIX="$(bashio::config 'discovery_prefix')"
export LOG_LEVEL="$(bashio::config 'log_level')"

exec python3 /broker.py

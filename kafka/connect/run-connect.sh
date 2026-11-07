#!/bin/sh
set -eu

config=/tmp/spark-connect-distributed.properties
advertised_host=${CONNECT_REST_ADVERTISED_HOST_NAME:-$(hostname)}

cp /opt/kafka/config/spark-connect-distributed.properties "$config"
printf '\nrest.advertised.host.name=%s\n' "$advertised_host" >> "$config"

exec /opt/kafka/bin/connect-distributed.sh "$config"

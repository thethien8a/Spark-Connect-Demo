FROM apache/kafka:4.3.0

ARG DEBEZIUM_POSTGRES_URL=https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.6.1.Final/debezium-connector-postgres-3.6.1.Final-plugin.tar.gz
ARG DEBEZIUM_POSTGRES_SHA512=774f6854a6c1c9a7ad3bdb6ddd367e9479a0b80e511ea27fc361320a955f9bf57a72393ff0e045b868b63589e7e16c27266cda296d9b1419bb071e914ee4d212

RUN set -eu; \
    archive=/tmp/debezium-connector-postgres-plugin.tar.gz; \
    plugin_dir=/opt/kafka/plugins/debezium-postgres; \
    wget --no-verbose --output-document="$archive" "$DEBEZIUM_POSTGRES_URL"; \
    echo "$DEBEZIUM_POSTGRES_SHA512  $archive" | sha512sum --check --strict; \
    mkdir -p "$plugin_dir"; \
    tar --extract --gzip --file="$archive" --directory="$plugin_dir"; \
    rm "$archive"

COPY --chown=appuser:appuser connect-distributed.properties /opt/kafka/config/spark-connect-distributed.properties
COPY --chown=appuser:appuser --chmod=755 run-connect.sh /opt/kafka/bin/run-connect.sh

EXPOSE 8083

CMD ["/opt/kafka/bin/run-connect.sh"]

#!/bin/sh
set -e

JARS_DIR="${JARS_DIR:-./jars}"
mkdir -p "$JARS_DIR"

download_if_missing() {
  target="$1"
  url="$2"
  if [ -s "$target" ]; then
    echo "Skipping $(basename "$target") (already present)"
  else
    echo "Downloading $(basename "$target")..."
    curl -fL --retry 3 -o "$target" "$url"
  fi
}

download_if_missing \
  "$JARS_DIR/postgresql-42.7.1.jar" \
  "https://jdbc.postgresql.org/download/postgresql-42.7.1.jar"

download_if_missing \
  "$JARS_DIR/clickhouse-jdbc-0.6.0-patch5-shaded.jar" \
  "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.0-patch5/clickhouse-jdbc-0.6.0-patch5-shaded.jar"

echo "JDBC drivers ready in $JARS_DIR/"
ls -la "$JARS_DIR/"

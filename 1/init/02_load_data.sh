#!/bin/bash
set -e

echo "Loading CSV data into raw_data table..."

for f in /data/MOCK_DATA*.csv; do
    echo "Loading: $f"
    psql -U postgres -d snowflake_lab -c "\COPY raw_data FROM '$f' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"')"
done

echo "Loaded rows:"
psql -U postgres -d snowflake_lab -c "SELECT COUNT(*) FROM raw_data;"

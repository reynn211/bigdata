#!/bin/bash
set -e

echo "Loading CSV data into raw_data table..."

for f in /data/*.csv; do
    echo "Loading file: $f"
    psql -U admin -d petstore -c "\COPY raw_data FROM '$f' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"')"
done

echo "Data loading complete."
psql -U admin -d petstore -c "SELECT COUNT(*) AS total_rows FROM raw_data;"

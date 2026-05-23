#!/bin/bash
set -e

CH_PWD="${CLICKHOUSE_PASSWORD:-clickhouse}"
CH=(clickhouse-client --password "$CH_PWD")

echo "Creating ClickHouse raw_data table..."

"${CH[@]}" --query "
CREATE TABLE IF NOT EXISTS default.raw_data (
    id UInt32,
    customer_first_name String,
    customer_last_name String,
    customer_age Nullable(UInt8),
    customer_email String,
    customer_country String,
    customer_postal_code Nullable(String),
    customer_pet_type String,
    customer_pet_name String,
    customer_pet_breed String,
    seller_first_name String,
    seller_last_name String,
    seller_email String,
    seller_country String,
    seller_postal_code Nullable(String),
    product_name String,
    product_category String,
    product_price Nullable(Float64),
    product_quantity Nullable(UInt32),
    sale_date String,
    sale_customer_id Nullable(UInt32),
    sale_seller_id Nullable(UInt32),
    sale_product_id Nullable(UInt32),
    sale_quantity Nullable(UInt32),
    sale_total_price Nullable(Float64),
    store_name String,
    store_location Nullable(String),
    store_city String,
    store_state Nullable(String),
    store_country String,
    store_phone Nullable(String),
    store_email Nullable(String),
    pet_category String,
    product_weight Nullable(Float64),
    product_color Nullable(String),
    product_size Nullable(String),
    product_brand Nullable(String),
    product_material Nullable(String),
    product_description Nullable(String),
    product_rating Nullable(Float64),
    product_reviews Nullable(UInt32),
    product_release_date Nullable(String),
    product_expiry_date Nullable(String),
    supplier_name Nullable(String),
    supplier_contact Nullable(String),
    supplier_email Nullable(String),
    supplier_phone Nullable(String),
    supplier_address Nullable(String),
    supplier_city Nullable(String),
    supplier_country Nullable(String)
) ENGINE = MergeTree()
ORDER BY id;
"

echo "Loading CSV files 0-4 into ClickHouse..."

for i in 0 1 2 3 4; do
    FILE="/data/mock_data_${i}.csv"
    if [ -f "$FILE" ]; then
        echo "Loading $FILE..."
        "${CH[@]}" --query "INSERT INTO default.raw_data FORMAT CSVWithNames" < "$FILE"
        echo "Loaded $FILE"
    else
        echo "WARNING: $FILE not found, skipping"
    fi
done

echo "ClickHouse data loading complete."
"${CH[@]}" --query "SELECT count(*) as total_rows FROM default.raw_data"

DROP TABLE IF EXISTS clickhouse.default.fact_sales;
DROP TABLE IF EXISTS clickhouse.default.dim_date;
DROP TABLE IF EXISTS clickhouse.default.dim_store;
DROP TABLE IF EXISTS clickhouse.default.dim_product;
DROP TABLE IF EXISTS clickhouse.default.dim_supplier;
DROP TABLE IF EXISTS clickhouse.default.dim_seller;
DROP TABLE IF EXISTS clickhouse.default.dim_customer;
DROP TABLE IF EXISTS clickhouse.default.dim_brand;
DROP TABLE IF EXISTS clickhouse.default.dim_category;
DROP TABLE IF EXISTS clickhouse.default.dim_country;

CREATE TABLE clickhouse.default.dim_country (
    country_id   INTEGER,
    country_name VARCHAR
)
WITH (engine = 'MergeTree', order_by = ARRAY['country_id']);

INSERT INTO clickhouse.default.dim_country
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY country_name) AS INTEGER),
    country_name
FROM (
    SELECT DISTINCT country_name
    FROM (
        SELECT customer_country AS country_name FROM clickhouse.default.raw_data
        UNION ALL
        SELECT seller_country   FROM clickhouse.default.raw_data
        UNION ALL
        SELECT store_country    FROM clickhouse.default.raw_data
        UNION ALL
        SELECT supplier_country FROM clickhouse.default.raw_data
        UNION ALL
        SELECT customer_country FROM postgresql.public.raw_data
        UNION ALL
        SELECT seller_country   FROM postgresql.public.raw_data
        UNION ALL
        SELECT store_country    FROM postgresql.public.raw_data
        UNION ALL
        SELECT supplier_country FROM postgresql.public.raw_data
    ) all_countries
    WHERE country_name IS NOT NULL AND country_name <> ''
) u;

CREATE TABLE clickhouse.default.dim_category (
    category_id   INTEGER,
    category_name VARCHAR
)
WITH (engine = 'MergeTree', order_by = ARRAY['category_id']);

INSERT INTO clickhouse.default.dim_category
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY category_name) AS INTEGER),
    category_name
FROM (
    SELECT DISTINCT product_category AS category_name
    FROM (
        SELECT product_category FROM clickhouse.default.raw_data
        UNION ALL
        SELECT product_category FROM postgresql.public.raw_data
    ) c
    WHERE product_category IS NOT NULL AND product_category <> ''
) u;

CREATE TABLE clickhouse.default.dim_brand (
    brand_id   INTEGER,
    brand_name VARCHAR
)
WITH (engine = 'MergeTree', order_by = ARRAY['brand_id']);

INSERT INTO clickhouse.default.dim_brand
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY brand_name) AS INTEGER),
    brand_name
FROM (
    SELECT DISTINCT product_brand AS brand_name
    FROM (
        SELECT product_brand FROM clickhouse.default.raw_data
        UNION ALL
        SELECT product_brand FROM postgresql.public.raw_data
    ) b
    WHERE product_brand IS NOT NULL AND product_brand <> ''
) u;

CREATE TABLE clickhouse.default.dim_customer (
    customer_id  INTEGER,
    first_name   VARCHAR,
    last_name    VARCHAR,
    age          INTEGER,
    email        VARCHAR,
    country_id   INTEGER,
    postal_code  VARCHAR,
    pet_type     VARCHAR,
    pet_name     VARCHAR,
    pet_breed    VARCHAR,
    pet_category VARCHAR
)
WITH (engine = 'MergeTree', order_by = ARRAY['customer_id']);

INSERT INTO clickhouse.default.dim_customer
SELECT
    d.customer_id, d.first_name, d.last_name, d.age, d.email,
    c.country_id,
    d.postal_code, d.pet_type, d.pet_name, d.pet_breed, d.pet_category
FROM (
    SELECT
        sale_customer_id          AS customer_id,
        customer_first_name       AS first_name,
        customer_last_name        AS last_name,
        CAST(customer_age AS INTEGER) AS age,
        customer_email            AS email,
        customer_country,
        customer_postal_code      AS postal_code,
        customer_pet_type         AS pet_type,
        customer_pet_name         AS pet_name,
        customer_pet_breed        AS pet_breed,
        pet_category,
        ROW_NUMBER() OVER (PARTITION BY sale_customer_id ORDER BY sale_customer_id) AS rn
    FROM (
        SELECT sale_customer_id, customer_first_name, customer_last_name,
               customer_age, customer_email, customer_country,
               customer_postal_code, customer_pet_type, customer_pet_name,
               customer_pet_breed, pet_category
        FROM clickhouse.default.raw_data
        UNION ALL
        SELECT sale_customer_id, customer_first_name, customer_last_name,
               CAST(customer_age AS INTEGER), customer_email, customer_country,
               customer_postal_code, customer_pet_type, customer_pet_name,
               customer_pet_breed, pet_category
        FROM postgresql.public.raw_data
    ) combined
    WHERE sale_customer_id IS NOT NULL
) d
LEFT JOIN clickhouse.default.dim_country c ON d.customer_country = c.country_name
WHERE d.rn = 1;

CREATE TABLE clickhouse.default.dim_seller (
    seller_id   INTEGER,
    first_name  VARCHAR,
    last_name   VARCHAR,
    email       VARCHAR,
    country_id  INTEGER,
    postal_code VARCHAR
)
WITH (engine = 'MergeTree', order_by = ARRAY['seller_id']);

INSERT INTO clickhouse.default.dim_seller
SELECT
    d.seller_id, d.first_name, d.last_name, d.email,
    c.country_id,
    d.postal_code
FROM (
    SELECT
        sale_seller_id        AS seller_id,
        seller_first_name     AS first_name,
        seller_last_name      AS last_name,
        seller_email          AS email,
        seller_country,
        seller_postal_code    AS postal_code,
        ROW_NUMBER() OVER (PARTITION BY sale_seller_id ORDER BY sale_seller_id) AS rn
    FROM (
        SELECT sale_seller_id, seller_first_name, seller_last_name,
               seller_email, seller_country, seller_postal_code
        FROM clickhouse.default.raw_data
        UNION ALL
        SELECT sale_seller_id, seller_first_name, seller_last_name,
               seller_email, seller_country, seller_postal_code
        FROM postgresql.public.raw_data
    ) combined
    WHERE sale_seller_id IS NOT NULL
) d
LEFT JOIN clickhouse.default.dim_country c ON d.seller_country = c.country_name
WHERE d.rn = 1;

CREATE TABLE clickhouse.default.dim_supplier (
    supplier_id   INTEGER,
    supplier_name VARCHAR,
    contact       VARCHAR,
    email         VARCHAR,
    phone         VARCHAR,
    address       VARCHAR,
    city          VARCHAR,
    country_id    INTEGER
)
WITH (engine = 'MergeTree', order_by = ARRAY['supplier_id']);

INSERT INTO clickhouse.default.dim_supplier
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY d.supplier_name, d.city) AS INTEGER) AS supplier_id,
    d.supplier_name, d.contact, d.email, d.phone, d.address, d.city,
    c.country_id
FROM (
    SELECT
        supplier_name,
        supplier_contact  AS contact,
        supplier_email    AS email,
        supplier_phone    AS phone,
        supplier_address  AS address,
        supplier_city     AS city,
        supplier_country,
        ROW_NUMBER() OVER (PARTITION BY supplier_name, supplier_city
                           ORDER BY supplier_name) AS rn
    FROM (
        SELECT supplier_name, supplier_contact, supplier_email, supplier_phone,
               supplier_address, supplier_city, supplier_country
        FROM clickhouse.default.raw_data
        UNION ALL
        SELECT supplier_name, supplier_contact, supplier_email, supplier_phone,
               supplier_address, supplier_city, supplier_country
        FROM postgresql.public.raw_data
    ) combined
    WHERE supplier_name IS NOT NULL AND supplier_name <> ''
) d
LEFT JOIN clickhouse.default.dim_country c ON d.supplier_country = c.country_name
WHERE d.rn = 1;

CREATE TABLE clickhouse.default.dim_product (
    product_id   INTEGER,
    product_name VARCHAR,
    category_id  INTEGER,
    price        DOUBLE,
    quantity     INTEGER,
    weight       DOUBLE,
    color        VARCHAR,
    size         VARCHAR,
    brand_id     INTEGER,
    material     VARCHAR,
    description  VARCHAR,
    rating       DOUBLE,
    reviews      INTEGER,
    release_date DATE,
    expiry_date  DATE,
    supplier_id  INTEGER
)
WITH (engine = 'MergeTree', order_by = ARRAY['product_id']);

INSERT INTO clickhouse.default.dim_product
SELECT
    d.product_id, d.product_name,
    cat.category_id,
    d.price, d.quantity, d.weight, d.color, d.size,
    br.brand_id,
    d.material, d.description, d.rating, d.reviews,
    d.release_date, d.expiry_date,
    sup.supplier_id
FROM (
    SELECT
        sale_product_id AS product_id,
        product_name,
        product_category,
        CAST(product_price AS DOUBLE)    AS price,
        CAST(product_quantity AS INTEGER) AS quantity,
        CAST(product_weight AS DOUBLE)   AS weight,
        product_color    AS color,
        product_size     AS size,
        product_brand,
        product_material AS material,
        product_description AS description,
        CAST(product_rating AS DOUBLE)   AS rating,
        CAST(product_reviews AS INTEGER) AS reviews,
        TRY(CAST(DATE_PARSE(product_release_date, '%m/%d/%Y') AS DATE)) AS release_date,
        TRY(CAST(DATE_PARSE(product_expiry_date,  '%m/%d/%Y') AS DATE)) AS expiry_date,
        supplier_name,
        supplier_city,
        ROW_NUMBER() OVER (PARTITION BY sale_product_id ORDER BY sale_product_id) AS rn
    FROM (
        SELECT sale_product_id, product_name, product_category,
               product_price, product_quantity, product_weight,
               product_color, product_size, product_brand, product_material,
               product_description, product_rating, product_reviews,
               product_release_date, product_expiry_date,
               supplier_name, supplier_city
        FROM clickhouse.default.raw_data
        UNION ALL
        SELECT sale_product_id, product_name, product_category,
               CAST(product_price AS DOUBLE), CAST(product_quantity AS INTEGER),
               CAST(product_weight AS DOUBLE), product_color, product_size,
               product_brand, product_material, product_description,
               CAST(product_rating AS DOUBLE), CAST(product_reviews AS INTEGER),
               product_release_date, product_expiry_date,
               supplier_name, supplier_city
        FROM postgresql.public.raw_data
    ) combined
    WHERE sale_product_id IS NOT NULL
) d
LEFT JOIN clickhouse.default.dim_category cat ON d.product_category = cat.category_name
LEFT JOIN clickhouse.default.dim_brand    br  ON d.product_brand    = br.brand_name
LEFT JOIN clickhouse.default.dim_supplier sup ON d.supplier_name    = sup.supplier_name
                                              AND d.supplier_city   = sup.city
WHERE d.rn = 1;

CREATE TABLE clickhouse.default.dim_store (
    store_id   INTEGER,
    store_name VARCHAR,
    location   VARCHAR,
    city       VARCHAR,
    state      VARCHAR,
    country_id INTEGER,
    phone      VARCHAR,
    email      VARCHAR
)
WITH (engine = 'MergeTree', order_by = ARRAY['store_id']);

INSERT INTO clickhouse.default.dim_store
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY d.store_name, d.city) AS INTEGER) AS store_id,
    d.store_name, d.location, d.city, d.state,
    c.country_id,
    d.phone, d.email
FROM (
    SELECT
        store_name,
        store_location AS location,
        store_city     AS city,
        store_state    AS state,
        store_country,
        store_phone    AS phone,
        store_email    AS email,
        ROW_NUMBER() OVER (PARTITION BY store_name, store_city
                           ORDER BY store_name) AS rn
    FROM (
        SELECT store_name, store_location, store_city, store_state,
               store_country, store_phone, store_email
        FROM clickhouse.default.raw_data
        UNION ALL
        SELECT store_name, store_location, store_city, store_state,
               store_country, store_phone, store_email
        FROM postgresql.public.raw_data
    ) combined
) d
LEFT JOIN clickhouse.default.dim_country c ON d.store_country = c.country_name
WHERE d.rn = 1;

CREATE TABLE clickhouse.default.dim_date (
    date_id    INTEGER,
    sale_date  DATE,
    sale_day   INTEGER,
    sale_month INTEGER,
    sale_year  INTEGER
)
WITH (engine = 'MergeTree', order_by = ARRAY['date_id']);

INSERT INTO clickhouse.default.dim_date
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY sale_dt) AS INTEGER) AS date_id,
    sale_dt  AS sale_date,
    CAST(DAY(sale_dt)   AS INTEGER) AS sale_day,
    CAST(MONTH(sale_dt) AS INTEGER) AS sale_month,
    CAST(YEAR(sale_dt)  AS INTEGER) AS sale_year
FROM (
    SELECT DISTINCT TRY(CAST(DATE_PARSE(sale_date, '%m/%d/%Y') AS DATE)) AS sale_dt
    FROM (
        SELECT sale_date FROM clickhouse.default.raw_data
        UNION ALL
        SELECT sale_date FROM postgresql.public.raw_data
    ) combined
    WHERE sale_date IS NOT NULL AND sale_date <> ''
) parsed
WHERE sale_dt IS NOT NULL;

CREATE TABLE clickhouse.default.fact_sales (
    sale_id          INTEGER,
    customer_id      INTEGER,
    seller_id        INTEGER,
    product_id       INTEGER,
    store_id         INTEGER,
    sale_date        DATE,
    sale_quantity     INTEGER,
    sale_total_price DOUBLE,
    source_system    VARCHAR
)
WITH (engine = 'MergeTree', order_by = ARRAY['sale_id']);

INSERT INTO clickhouse.default.fact_sales
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY combined.src, combined.raw_id) AS INTEGER) AS sale_id,
    combined.customer_id,
    combined.seller_id,
    combined.product_id,
    s.store_id,
    combined.sale_dt AS sale_date,
    combined.sale_quantity,
    combined.sale_total_price,
    combined.source_system
FROM (
    SELECT
        id AS raw_id,
        sale_customer_id AS customer_id,
        sale_seller_id   AS seller_id,
        sale_product_id  AS product_id,
        store_name,
        store_city,
        TRY(CAST(DATE_PARSE(sale_date, '%m/%d/%Y') AS DATE)) AS sale_dt,
        CAST(sale_quantity AS INTEGER)    AS sale_quantity,
        CAST(sale_total_price AS DOUBLE) AS sale_total_price,
        'clickhouse' AS source_system,
        1 AS src
    FROM clickhouse.default.raw_data
    UNION ALL
    SELECT
        id AS raw_id,
        sale_customer_id AS customer_id,
        sale_seller_id   AS seller_id,
        sale_product_id  AS product_id,
        store_name,
        store_city,
        TRY(CAST(DATE_PARSE(sale_date, '%m/%d/%Y') AS DATE)) AS sale_dt,
        sale_quantity,
        CAST(sale_total_price AS DOUBLE) AS sale_total_price,
        'postgresql' AS source_system,
        2 AS src
    FROM postgresql.public.raw_data
) combined
LEFT JOIN clickhouse.default.dim_store s
    ON combined.store_name = s.store_name AND combined.store_city = s.city;

DROP TABLE IF EXISTS clickhouse.default.report_product_sales;

CREATE TABLE clickhouse.default.report_product_sales (
    product_name       VARCHAR,
    category           VARCHAR,
    total_revenue      DOUBLE,
    total_quantity_sold INTEGER,
    avg_rating         DOUBLE,
    avg_reviews        DOUBLE,
    product_rank       INTEGER
)
WITH (engine = 'MergeTree', order_by = ARRAY['product_rank']);

INSERT INTO clickhouse.default.report_product_sales
SELECT
    p.product_name,
    cat.category_name AS category,
    SUM(f.sale_total_price) AS total_revenue,
    CAST(SUM(f.sale_quantity) AS INTEGER) AS total_quantity_sold,
    AVG(p.rating) AS avg_rating,
    AVG(CAST(p.reviews AS DOUBLE)) AS avg_reviews,
    CAST(ROW_NUMBER() OVER (ORDER BY SUM(f.sale_total_price) DESC) AS INTEGER) AS product_rank
FROM clickhouse.default.fact_sales f
JOIN clickhouse.default.dim_product  p   ON f.product_id  = p.product_id
LEFT JOIN clickhouse.default.dim_category cat ON p.category_id = cat.category_id
GROUP BY p.product_name, cat.category_name
ORDER BY total_revenue DESC
LIMIT 10;

DROP TABLE IF EXISTS clickhouse.default.report_customer_sales;

CREATE TABLE clickhouse.default.report_customer_sales (
    customer_name VARCHAR,
    country       VARCHAR,
    total_spent   DOUBLE,
    total_orders  INTEGER,
    avg_check     DOUBLE,
    customer_rank INTEGER
)
WITH (engine = 'MergeTree', order_by = ARRAY['customer_rank']);

INSERT INTO clickhouse.default.report_customer_sales
SELECT
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    co.country_name AS country,
    SUM(f.sale_total_price) AS total_spent,
    CAST(COUNT(*) AS INTEGER) AS total_orders,
    AVG(f.sale_total_price) AS avg_check,
    CAST(ROW_NUMBER() OVER (ORDER BY SUM(f.sale_total_price) DESC) AS INTEGER) AS customer_rank
FROM clickhouse.default.fact_sales f
JOIN clickhouse.default.dim_customer c  ON f.customer_id = c.customer_id
LEFT JOIN clickhouse.default.dim_country co ON c.country_id  = co.country_id
GROUP BY c.first_name, c.last_name, co.country_name
ORDER BY total_spent DESC
LIMIT 10;

DROP TABLE IF EXISTS clickhouse.default.report_time_sales;

CREATE TABLE clickhouse.default.report_time_sales (
    sale_year       INTEGER,
    sale_month      INTEGER,
    total_revenue   DOUBLE,
    total_orders    INTEGER,
    avg_order_value DOUBLE,
    total_quantity  INTEGER
)
WITH (engine = 'MergeTree', order_by = ARRAY['sale_year', 'sale_month']);

INSERT INTO clickhouse.default.report_time_sales
SELECT
    d.sale_year,
    d.sale_month,
    SUM(f.sale_total_price) AS total_revenue,
    CAST(COUNT(*) AS INTEGER) AS total_orders,
    AVG(f.sale_total_price) AS avg_order_value,
    CAST(SUM(f.sale_quantity) AS INTEGER) AS total_quantity
FROM clickhouse.default.fact_sales f
JOIN clickhouse.default.dim_date d ON f.sale_date = d.sale_date
GROUP BY d.sale_year, d.sale_month
ORDER BY d.sale_year, d.sale_month;

DROP TABLE IF EXISTS clickhouse.default.report_store_sales;

CREATE TABLE clickhouse.default.report_store_sales (
    store_name    VARCHAR,
    city          VARCHAR,
    country       VARCHAR,
    total_revenue DOUBLE,
    total_orders  INTEGER,
    avg_check     DOUBLE,
    store_rank    INTEGER
)
WITH (engine = 'MergeTree', order_by = ARRAY['store_rank']);

INSERT INTO clickhouse.default.report_store_sales
SELECT
    s.store_name,
    s.city,
    co.country_name AS country,
    SUM(f.sale_total_price) AS total_revenue,
    CAST(COUNT(*) AS INTEGER) AS total_orders,
    AVG(f.sale_total_price) AS avg_check,
    CAST(ROW_NUMBER() OVER (ORDER BY SUM(f.sale_total_price) DESC) AS INTEGER) AS store_rank
FROM clickhouse.default.fact_sales f
JOIN clickhouse.default.dim_store   s  ON f.store_id   = s.store_id
LEFT JOIN clickhouse.default.dim_country co ON s.country_id = co.country_id
GROUP BY s.store_name, s.city, co.country_name
ORDER BY total_revenue DESC
LIMIT 5;

DROP TABLE IF EXISTS clickhouse.default.report_supplier_sales;

CREATE TABLE clickhouse.default.report_supplier_sales (
    supplier_name      VARCHAR,
    country            VARCHAR,
    total_revenue      DOUBLE,
    total_products_sold INTEGER,
    avg_product_price  DOUBLE,
    supplier_rank      INTEGER
)
WITH (engine = 'MergeTree', order_by = ARRAY['supplier_rank']);

INSERT INTO clickhouse.default.report_supplier_sales
SELECT
    sup.supplier_name,
    co.country_name AS country,
    SUM(f.sale_total_price) AS total_revenue,
    CAST(SUM(f.sale_quantity) AS INTEGER) AS total_products_sold,
    AVG(p.price) AS avg_product_price,
    CAST(ROW_NUMBER() OVER (ORDER BY SUM(f.sale_total_price) DESC) AS INTEGER) AS supplier_rank
FROM clickhouse.default.fact_sales f
JOIN clickhouse.default.dim_product  p   ON f.product_id   = p.product_id
JOIN clickhouse.default.dim_supplier sup ON p.supplier_id   = sup.supplier_id
LEFT JOIN clickhouse.default.dim_country co ON sup.country_id = co.country_id
GROUP BY sup.supplier_name, co.country_name
ORDER BY total_revenue DESC
LIMIT 5;

DROP TABLE IF EXISTS clickhouse.default.report_product_quality;

CREATE TABLE clickhouse.default.report_product_quality (
    product_name            VARCHAR,
    category                VARCHAR,
    brand                   VARCHAR,
    rating                  DOUBLE,
    reviews                 INTEGER,
    total_revenue           DOUBLE,
    total_quantity_sold      INTEGER,
    revenue_per_rating_point DOUBLE
)
WITH (engine = 'MergeTree', order_by = ARRAY['rating', 'product_name']);

INSERT INTO clickhouse.default.report_product_quality
SELECT
    p.product_name,
    cat.category_name AS category,
    br.brand_name     AS brand,
    p.rating,
    p.reviews,
    SUM(f.sale_total_price) AS total_revenue,
    CAST(SUM(f.sale_quantity) AS INTEGER) AS total_quantity_sold,
    CASE WHEN p.rating > 0 THEN SUM(f.sale_total_price) / p.rating ELSE 0 END AS revenue_per_rating_point
FROM clickhouse.default.fact_sales f
JOIN clickhouse.default.dim_product  p   ON f.product_id  = p.product_id
LEFT JOIN clickhouse.default.dim_category cat ON p.category_id = cat.category_id
LEFT JOIN clickhouse.default.dim_brand    br  ON p.brand_id    = br.brand_id
WHERE p.rating IS NOT NULL
GROUP BY p.product_name, cat.category_name, br.brand_name, p.rating, p.reviews
ORDER BY p.rating DESC, total_revenue DESC;


DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;

CREATE TABLE dim_supplier (
    supplier_name VARCHAR(200),
    supplier_city VARCHAR(100),
    contact       VARCHAR(200),
    email         VARCHAR(200),
    phone         VARCHAR(50),
    address       VARCHAR(300),
    country       VARCHAR(100),
    PRIMARY KEY (supplier_name, supplier_city)
);

CREATE TABLE dim_customer (
    customer_id   INTEGER PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    age           INTEGER,
    email         VARCHAR(200),
    country       VARCHAR(100),
    postal_code   VARCHAR(50),
    pet_type      VARCHAR(50),
    pet_name      VARCHAR(100),
    pet_breed     VARCHAR(100)
);

CREATE TABLE dim_seller (
    seller_id     INTEGER PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    email         VARCHAR(200),
    country       VARCHAR(100),
    postal_code   VARCHAR(50)
);

CREATE TABLE dim_product (
    product_id    INTEGER PRIMARY KEY,
    product_name  VARCHAR(200),
    category      VARCHAR(100),
    price         NUMERIC(10, 2),
    quantity      INTEGER,
    pet_category  VARCHAR(50),
    weight        NUMERIC(10, 2),
    color         VARCHAR(50),
    size          VARCHAR(50),
    brand         VARCHAR(100),
    material      VARCHAR(100),
    description   TEXT,
    rating        NUMERIC(3, 1),
    reviews       INTEGER,
    release_date  DATE,
    expiry_date   DATE,
    supplier_name VARCHAR(200),
    supplier_city VARCHAR(100)
);

CREATE TABLE dim_store (
    store_name    VARCHAR(200),
    store_city    VARCHAR(100),
    location      VARCHAR(200),
    state         VARCHAR(100),
    country       VARCHAR(100),
    phone         VARCHAR(50),
    email         VARCHAR(200),
    PRIMARY KEY (store_name, store_city)
);

CREATE TABLE fact_sales (
    sale_id            INTEGER PRIMARY KEY,
    customer_id        INTEGER,
    seller_id          INTEGER,
    product_id         INTEGER,
    store_name         VARCHAR(200),
    store_city         VARCHAR(100),
    sale_date          DATE,
    sale_quantity      INTEGER,
    sale_total_price   NUMERIC(12, 2)
);

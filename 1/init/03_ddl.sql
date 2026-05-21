DROP TABLE IF EXISTS fact_sales       CASCADE;
DROP TABLE IF EXISTS dim_customer     CASCADE;
DROP TABLE IF EXISTS dim_seller       CASCADE;
DROP TABLE IF EXISTS dim_product      CASCADE;
DROP TABLE IF EXISTS dim_store        CASCADE;
DROP TABLE IF EXISTS dim_supplier     CASCADE;
DROP TABLE IF EXISTS dim_country      CASCADE;
DROP TABLE IF EXISTS dim_brand        CASCADE;
DROP TABLE IF EXISTS dim_category     CASCADE;
DROP TABLE IF EXISTS dim_pet_category CASCADE;
DROP TABLE IF EXISTS dim_pet_type     CASCADE;
DROP TABLE IF EXISTS dim_pet_breed    CASCADE;
DROP TABLE IF EXISTS dim_material     CASCADE;

-- нормализованные справочники

CREATE TABLE dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_brand (
    brand_id   SERIAL PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_category (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_pet_category (
    pet_category_id   SERIAL PRIMARY KEY,
    pet_category_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE dim_pet_type (
    pet_type_id   SERIAL PRIMARY KEY,
    pet_type_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE dim_pet_breed (
    breed_id   SERIAL PRIMARY KEY,
    breed_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_material (
    material_id   SERIAL PRIMARY KEY,
    material_name VARCHAR(100) NOT NULL UNIQUE
);

-- измерения

CREATE TABLE dim_supplier (
    supplier_id   SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200) NOT NULL UNIQUE,
    contact_name  VARCHAR(200),
    email         VARCHAR(200),
    phone         VARCHAR(50),
    address       VARCHAR(300),
    city          VARCHAR(100),
    country_id    INT REFERENCES dim_country(country_id)
);

CREATE TABLE dim_customer (
    customer_id  INT PRIMARY KEY,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    age          INT,
    email        VARCHAR(200),
    postal_code  VARCHAR(50),
    country_id   INT REFERENCES dim_country(country_id),
    pet_type_id  INT REFERENCES dim_pet_type(pet_type_id),
    pet_name     VARCHAR(100),
    breed_id     INT REFERENCES dim_pet_breed(breed_id)
);

CREATE TABLE dim_seller (
    seller_id   INT PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(200),
    postal_code VARCHAR(50),
    country_id  INT REFERENCES dim_country(country_id)
);

CREATE TABLE dim_product (
    product_id      INT PRIMARY KEY,
    product_name    VARCHAR(200),
    category_id     INT REFERENCES dim_category(category_id),
    price           DECIMAL(10,2),
    quantity        INT,
    pet_category_id INT REFERENCES dim_pet_category(pet_category_id),
    weight          DECIMAL(10,2),
    color           VARCHAR(50),
    size            VARCHAR(50),
    brand_id        INT REFERENCES dim_brand(brand_id),
    material_id     INT REFERENCES dim_material(material_id),
    description     TEXT,
    rating          DECIMAL(3,1),
    reviews         INT,
    release_date    DATE,
    expiry_date     DATE,
    supplier_id     INT REFERENCES dim_supplier(supplier_id)
);

CREATE TABLE dim_store (
    store_id   SERIAL PRIMARY KEY,
    store_name VARCHAR(200) NOT NULL,
    location   VARCHAR(200),
    city       VARCHAR(100),
    state      VARCHAR(100),
    country_id INT REFERENCES dim_country(country_id),
    phone      VARCHAR(50),
    email      VARCHAR(200),
    UNIQUE (store_name)
);

-- таблица фактов

CREATE TABLE fact_sales (
    sale_id          BIGSERIAL PRIMARY KEY,
    source_id        BIGINT,
    customer_id      INT REFERENCES dim_customer(customer_id),
    seller_id        INT REFERENCES dim_seller(seller_id),
    product_id       INT REFERENCES dim_product(product_id),
    store_id         INT REFERENCES dim_store(store_id),
    sale_date        DATE,
    sale_quantity    INT,
    sale_total_price DECIMAL(10,2)
);

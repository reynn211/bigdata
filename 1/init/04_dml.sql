-- 1. Sub-dimensions

INSERT INTO dim_country (country_name)
SELECT DISTINCT name
FROM (
    SELECT customer_country AS name FROM raw_data
    UNION ALL SELECT seller_country    FROM raw_data
    UNION ALL SELECT store_country     FROM raw_data
    UNION ALL SELECT supplier_country  FROM raw_data
) src
WHERE name IS NOT NULL AND name <> ''
ON CONFLICT (country_name) DO NOTHING;

INSERT INTO dim_brand (brand_name)
SELECT DISTINCT product_brand FROM raw_data
WHERE product_brand IS NOT NULL AND product_brand <> ''
ON CONFLICT (brand_name) DO NOTHING;

INSERT INTO dim_category (category_name)
SELECT DISTINCT product_category FROM raw_data
WHERE product_category IS NOT NULL AND product_category <> ''
ON CONFLICT (category_name) DO NOTHING;

INSERT INTO dim_pet_category (pet_category_name)
SELECT DISTINCT pet_category FROM raw_data
WHERE pet_category IS NOT NULL AND pet_category <> ''
ON CONFLICT (pet_category_name) DO NOTHING;

INSERT INTO dim_pet_type (pet_type_name)
SELECT DISTINCT customer_pet_type FROM raw_data
WHERE customer_pet_type IS NOT NULL AND customer_pet_type <> ''
ON CONFLICT (pet_type_name) DO NOTHING;

INSERT INTO dim_pet_breed (breed_name)
SELECT DISTINCT customer_pet_breed FROM raw_data
WHERE customer_pet_breed IS NOT NULL AND customer_pet_breed <> ''
ON CONFLICT (breed_name) DO NOTHING;

INSERT INTO dim_material (material_name)
SELECT DISTINCT product_material FROM raw_data
WHERE product_material IS NOT NULL AND product_material <> ''
ON CONFLICT (material_name) DO NOTHING;

-- 2. Suppliers (загружаются перед продуктами: dim_product.supplier_id зависит от него)

INSERT INTO dim_supplier (supplier_name, contact_name, email, phone, address, city, country_id)
SELECT DISTINCT ON (r.supplier_name)
    r.supplier_name,
    r.supplier_contact,
    r.supplier_email,
    r.supplier_phone,
    r.supplier_address,
    r.supplier_city,
    c.country_id
FROM raw_data r
LEFT JOIN dim_country c ON c.country_name = r.supplier_country
WHERE r.supplier_name IS NOT NULL AND r.supplier_name <> ''
ORDER BY r.supplier_name
ON CONFLICT (supplier_name) DO NOTHING;

-- 3. Customers

INSERT INTO dim_customer (customer_id, first_name, last_name, age, email, postal_code,
                          country_id, pet_type_id, pet_name, breed_id)
SELECT DISTINCT ON (r.sale_customer_id)
    r.sale_customer_id,
    r.customer_first_name,
    r.customer_last_name,
    r.customer_age,
    r.customer_email,
    r.customer_postal_code,
    c.country_id,
    pt.pet_type_id,
    r.customer_pet_name,
    pb.breed_id
FROM raw_data r
LEFT JOIN dim_country   c  ON c.country_name    = r.customer_country
LEFT JOIN dim_pet_type  pt ON pt.pet_type_name  = r.customer_pet_type
LEFT JOIN dim_pet_breed pb ON pb.breed_name     = r.customer_pet_breed
WHERE r.sale_customer_id IS NOT NULL
ORDER BY r.sale_customer_id
ON CONFLICT (customer_id) DO NOTHING;

-- 4. Sellers

INSERT INTO dim_seller (seller_id, first_name, last_name, email, postal_code, country_id)
SELECT DISTINCT ON (r.sale_seller_id)
    r.sale_seller_id,
    r.seller_first_name,
    r.seller_last_name,
    r.seller_email,
    r.seller_postal_code,
    c.country_id
FROM raw_data r
LEFT JOIN dim_country c ON c.country_name = r.seller_country
WHERE r.sale_seller_id IS NOT NULL
ORDER BY r.sale_seller_id
ON CONFLICT (seller_id) DO NOTHING;

-- 5. Products

INSERT INTO dim_product (product_id, product_name, category_id, price, quantity,
                         pet_category_id, weight, color, size, brand_id, material_id,
                         description, rating, reviews, release_date, expiry_date, supplier_id)
SELECT DISTINCT ON (r.sale_product_id)
    r.sale_product_id,
    r.product_name,
    cat.category_id,
    r.product_price,
    r.product_quantity,
    pc.pet_category_id,
    r.product_weight,
    r.product_color,
    r.product_size,
    b.brand_id,
    m.material_id,
    r.product_description,
    r.product_rating,
    r.product_reviews,
    CASE WHEN r.product_release_date ~ '^[0-9]+/[0-9]+/[0-9]+$'
         THEN TO_DATE(r.product_release_date, 'MM/DD/YYYY')
         ELSE NULL END,
    CASE WHEN r.product_expiry_date ~ '^[0-9]+/[0-9]+/[0-9]+$'
         THEN TO_DATE(r.product_expiry_date, 'MM/DD/YYYY')
         ELSE NULL END,
    sup.supplier_id
FROM raw_data r
LEFT JOIN dim_category     cat ON cat.category_name    = r.product_category
LEFT JOIN dim_pet_category pc  ON pc.pet_category_name = r.pet_category
LEFT JOIN dim_brand        b   ON b.brand_name         = r.product_brand
LEFT JOIN dim_material     m   ON m.material_name      = r.product_material
LEFT JOIN dim_supplier     sup ON sup.supplier_name    = r.supplier_name
WHERE r.sale_product_id IS NOT NULL
ORDER BY r.sale_product_id
ON CONFLICT (product_id) DO NOTHING;

-- 6. Stores

INSERT INTO dim_store (store_name, location, city, state, country_id, phone, email)
SELECT DISTINCT ON (r.store_name)
    r.store_name,
    r.store_location,
    r.store_city,
    r.store_state,
    c.country_id,
    r.store_phone,
    r.store_email
FROM raw_data r
LEFT JOIN dim_country c ON c.country_name = r.store_country
WHERE r.store_name IS NOT NULL AND r.store_name <> ''
ORDER BY r.store_name
ON CONFLICT (store_name) DO NOTHING;

-- 7. Fact

INSERT INTO fact_sales (source_id, customer_id, seller_id, product_id, store_id,
                        sale_date, sale_quantity, sale_total_price)
SELECT
    r.id,
    r.sale_customer_id,
    r.sale_seller_id,
    r.sale_product_id,
    s.store_id,
    CASE WHEN r.sale_date ~ '^[0-9]+/[0-9]+/[0-9]+$'
         THEN TO_DATE(r.sale_date, 'MM/DD/YYYY')
         ELSE NULL END,
    r.sale_quantity,
    r.sale_total_price
FROM raw_data r
LEFT JOIN dim_store s ON s.store_name = r.store_name;

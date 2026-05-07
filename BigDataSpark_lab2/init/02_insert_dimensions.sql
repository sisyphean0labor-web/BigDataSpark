-- ============================================
-- Заполнение dim_customer (уникальность по email)
-- ============================================
INSERT INTO dim_customer (customer_email, first_name, last_name, age, country, postal_code, pet_type, pet_name, pet_breed)
SELECT DISTINCT ON (customer_email)
    customer_email,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM mock_data_raw
WHERE customer_email IS NOT NULL
ON CONFLICT (customer_email) DO NOTHING;

-- ============================================
-- Заполнение dim_seller (уникальность по email)
-- ============================================
INSERT INTO dim_seller (seller_email, first_name, last_name, country, postal_code)
SELECT DISTINCT ON (seller_email)
    seller_email,
    seller_first_name,
    seller_last_name,
    seller_country,
    seller_postal_code
FROM mock_data_raw
WHERE seller_email IS NOT NULL
ON CONFLICT (seller_email) DO NOTHING;

-- ============================================
-- Заполнение dim_product (уникальность по name + brand)
-- ============================================
INSERT INTO dim_product (product_name, product_brand, category, price, weight, color, size, material, rating, reviews, release_date, expiry_date, pet_category, description)
SELECT DISTINCT ON (product_name, product_brand)
    product_name,
    product_brand,
    product_category,
    product_price,
    product_weight,
    product_color,
    product_size,
    product_material,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date,
    pet_category,
    product_description
FROM mock_data_raw
WHERE product_name IS NOT NULL
ON CONFLICT (product_name, product_brand) DO NOTHING;

-- ============================================
-- Заполнение dim_store (уникальность по name + email)
-- ============================================
INSERT INTO dim_store (store_name, store_email, location, city, state, country, phone)
SELECT DISTINCT ON (store_name, store_email)
    store_name,
    store_email,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone
FROM mock_data_raw
WHERE store_name IS NOT NULL
ON CONFLICT (store_name, store_email) DO NOTHING;

-- ============================================
-- Заполнение dim_supplier (уникальность по name)
-- ============================================
INSERT INTO dim_supplier (supplier_name, contact, email, phone, address, city, country)
SELECT DISTINCT ON (supplier_name)
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data_raw
WHERE supplier_name IS NOT NULL
ON CONFLICT (supplier_name) DO NOTHING;
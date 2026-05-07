-- ============================================
-- Шаг 1: Вставляем базовые данные (source_id, quantity, total_price)
-- ============================================
INSERT INTO fact_sales (source_id, quantity, total_price)
SELECT id, sale_quantity, sale_total_price
FROM mock_data_raw
WHERE sale_quantity IS NOT NULL;

-- ============================================
-- Шаг 2: Обновляем customer_id по email
-- ============================================
UPDATE fact_sales f
SET customer_id = c.customer_id
FROM mock_data_raw r
JOIN dim_customer c ON r.customer_email = c.customer_email
WHERE f.source_id = r.id;

-- ============================================
-- Шаг 3: Обновляем seller_id по email
-- ============================================
UPDATE fact_sales f
SET seller_id = s.seller_id
FROM mock_data_raw r
JOIN dim_seller s ON r.seller_email = s.seller_email
WHERE f.source_id = r.id;

-- ============================================
-- Шаг 4: Обновляем product_id по name + brand
-- ============================================
UPDATE fact_sales f
SET product_id = p.product_id
FROM mock_data_raw r
JOIN dim_product p ON r.product_name = p.product_name AND r.product_brand = p.product_brand
WHERE f.source_id = r.id;

-- ============================================
-- Шаг 5: Обновляем store_id по name + email
-- ============================================
UPDATE fact_sales f
SET store_id = s.store_id
FROM mock_data_raw r
JOIN dim_store s ON r.store_name = s.store_name AND r.store_email = s.store_email
WHERE f.source_id = r.id;

-- ============================================
-- Шаг 6: Обновляем supplier_id по name
-- ============================================
UPDATE fact_sales f
SET supplier_id = su.supplier_id
FROM mock_data_raw r
JOIN dim_supplier su ON r.supplier_name = su.supplier_name
WHERE f.source_id = r.id;

-- ============================================
-- Шаг 7: Обновляем date_id по sale_date
-- ============================================
UPDATE fact_sales f
SET date_id = d.date_id
FROM mock_data_raw r
JOIN dim_date d ON r.sale_date = d.full_date
WHERE f.source_id = r.id;
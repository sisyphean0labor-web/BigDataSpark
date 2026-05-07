-- Заполнение dim_date (календарь)
WITH all_dates AS (
    SELECT DISTINCT sale_date AS date_value FROM mock_data_raw WHERE sale_date IS NOT NULL
    UNION
    SELECT DISTINCT product_release_date FROM mock_data_raw WHERE product_release_date IS NOT NULL
    UNION
    SELECT DISTINCT product_expiry_date FROM mock_data_raw WHERE product_expiry_date IS NOT NULL
)
INSERT INTO dim_date (date_id, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
SELECT 
    EXTRACT(YEAR FROM date_value) * 10000 + EXTRACT(MONTH FROM date_value) * 100 + EXTRACT(DAY FROM date_value) AS date_id,
    date_value AS full_date,
    EXTRACT(YEAR FROM date_value) AS year,
    CEIL(EXTRACT(MONTH FROM date_value) / 3.0) AS quarter,
    EXTRACT(MONTH FROM date_value) AS month,
    TO_CHAR(date_value, 'Month') AS month_name,
    EXTRACT(DAY FROM date_value) AS day,
    EXTRACT(DOW FROM date_value) AS day_of_week,
    TO_CHAR(date_value, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM date_value) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM all_dates;
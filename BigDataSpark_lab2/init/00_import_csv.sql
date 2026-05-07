-- Создаём таблицу для исходных данных
DROP TABLE IF EXISTS mock_data_raw CASCADE;

CREATE TABLE mock_data_raw (
    id INT,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_age INT,
    customer_email VARCHAR(255),
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(50),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100),
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(255),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(50),
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    product_price NUMERIC(10,2),
    product_quantity INT,
    sale_date DATE,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price NUMERIC(10,2),
    store_name VARCHAR(255),
    store_location VARCHAR(255),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(255),
    pet_category VARCHAR(50),
    product_weight NUMERIC(10,2),
    product_color VARCHAR(50),
    product_size VARCHAR(50),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_description TEXT,
    product_rating NUMERIC(3,1),
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR(255),
    supplier_contact VARCHAR(255),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(255),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);

-- Импортируем CSV-файлы (пути изменены на /csv_data/)
COPY mock_data_raw FROM '/csv_data/mock_data1.csv' DELIMITER ',' CSV HEADER;
COPY mock_data_raw FROM '/csv_data/mock_data2.csv' DELIMITER ',' CSV HEADER;
COPY mock_data_raw FROM '/csv_data/mock_data3.csv' DELIMITER ',' CSV HEADER;
COPY mock_data_raw FROM '/csv_data/mock_data4.csv' DELIMITER ',' CSV HEADER;
COPY mock_data_raw FROM '/csv_data/mock_data5.csv' DELIMITER ',' CSV HEADER;
COPY mock_data_raw FROM '/csv_data/mock_data6.csv' DELIMITER ',' CSV HEADER;
COPY mock_data_raw FROM '/csv_data/mock_data7.csv' DELIMITER ',' CSV HEADER;
COPY mock_data_raw FROM '/csv_data/mock_data8.csv' DELIMITER ',' CSV HEADER;
COPY mock_data_raw FROM '/csv_data/mock_data9.csv' DELIMITER ',' CSV HEADER;
COPY mock_data_raw FROM '/csv_data/mock_data10.csv' DELIMITER ',' CSV HEADER;

-- Проверка (опционально)
SELECT COUNT(*) AS imported_rows FROM mock_data_raw;
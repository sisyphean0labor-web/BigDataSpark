from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, desc, row_number
from pyspark.sql.window import Window

# 1. Инициализация Spark
spark = SparkSession.builder \
    .appName("BDS_Lab2_Reports") \
    .config("spark.jars", "/opt/spark-apps/postgresql-42.7.3.jar,/opt/spark-apps/clickhouse-jdbc-0.4.6-all.jar") \
    .getOrCreate()

# 2. Параметры подключения к PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/bds_lab"
pg_properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# 3. Чтение таблиц
fact_sales = spark.read.jdbc(url=jdbc_url, table="fact_sales", properties=pg_properties)
dim_product = spark.read.jdbc(url=jdbc_url, table="dim_product", properties=pg_properties)
dim_customer = spark.read.jdbc(url=jdbc_url, table="dim_customer", properties=pg_properties)
dim_date = spark.read.jdbc(url=jdbc_url, table="dim_date", properties=pg_properties)
dim_store = spark.read.jdbc(url=jdbc_url, table="dim_store", properties=pg_properties)
dim_supplier = spark.read.jdbc(url=jdbc_url, table="dim_supplier", properties=pg_properties)

# 4. Объединение фактов с измерениями
sales_with_product = fact_sales.join(dim_product, "product_id")
sales_with_customer = fact_sales.join(dim_customer, "customer_id")
sales_with_date = fact_sales.join(dim_date, "date_id")
sales_with_store = fact_sales.join(dim_store, "store_id")
sales_with_supplier = fact_sales.join(dim_supplier, "supplier_id")

# ==================================================
# ВИТРИНА 1: Продажи по продуктам
# ==================================================

# 1.1 Топ-10 продуктов по выручке
top_10_products = sales_with_product.groupBy("product_name", "product_brand") \
    .agg(_sum("total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(10)

# 1.2 Общая выручка по категориям
revenue_by_category = sales_with_product.groupBy("category") \
    .agg(_sum("total_price").alias("total_revenue"))

# 1.3 Средний рейтинг и количество отзывов по продуктам
product_rating_reviews = dim_product.select("product_name", "product_brand", "rating", "reviews")

# ==================================================
# ВИТРИНА 2: Продажи по клиентам
# ==================================================

# 2.1 Топ-10 клиентов по сумме покупок
top_10_customers = sales_with_customer.groupBy("customer_id", "first_name", "last_name", "email") \
    .agg(_sum("total_price").alias("total_spent")) \
    .orderBy(desc("total_spent")) \
    .limit(10)

# 2.2 Распределение клиентов по странам
customers_by_country = dim_customer.groupBy("country") \
    .agg(count("*").alias("customer_count"))

# 2.3 Средний чек по клиентам
avg_check_per_customer = sales_with_customer.groupBy("customer_id", "first_name", "last_name") \
    .agg(avg("total_price").alias("avg_check"))

# ==================================================
# ВИТРИНА 3: Продажи по времени
# ==================================================

# 3.1 Месячные и годовые тренды продаж
monthly_trends = sales_with_date.groupBy("year", "month", "month_name") \
    .agg(_sum("total_price").alias("monthly_revenue"))

# 3.2 Сравнение выручки за разные периоды (по годам)
yearly_revenue = sales_with_date.groupBy("year") \
    .agg(_sum("total_price").alias("yearly_revenue"))

# 3.3 Средний чек по месяцам
avg_order_by_month = sales_with_date.groupBy("year", "month", "month_name") \
    .agg(avg("total_price").alias("avg_order_value"))

# ==================================================
# ВИТРИНА 4: Продажи по магазинам
# ==================================================

# 4.1 Топ-5 магазинов по выручке
top_5_stores = sales_with_store.groupBy("store_name", "city", "country") \
    .agg(_sum("total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(5)

# 4.2 Распределение продаж по городам и странам
sales_by_city_country = sales_with_store.groupBy("country", "city") \
    .agg(_sum("total_price").alias("revenue"), count("*").alias("transactions"))

# 4.3 Средний чек по магазинам
avg_check_by_store = sales_with_store.groupBy("store_name") \
    .agg(avg("total_price").alias("avg_check"))

# ==================================================
# ВИТРИНА 5: Продажи по поставщикам
# ==================================================

# 5.1 Топ-5 поставщиков по выручке
top_5_suppliers = sales_with_supplier.groupBy("supplier_name") \
    .agg(_sum("total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(5)

# 5.2 Средняя цена товаров от каждого поставщика
avg_price_by_supplier = dim_product.join(dim_supplier, dim_product.supplier_id == dim_supplier.supplier_id, how="inner") \
    .groupBy("supplier_name") \
    .agg(avg("price").alias("avg_product_price"))

# 5.3 Распределение продаж по странам поставщиков
sales_by_supplier_country = sales_with_supplier.groupBy("country") \
    .agg(_sum("total_price").alias("revenue"))

# ==================================================
# ВИТРИНА 6: Качество продукции
# ==================================================

# 6.1 Продукты с наивысшим и наименьшим рейтингом
highest_rated = dim_product.orderBy(desc("rating")).limit(5)
lowest_rated = dim_product.orderBy("rating").limit(5)

# 6.2 Корреляция между рейтингом и объемом продаж
correlation_data = sales_with_product.groupBy("product_id", "rating") \
    .agg(_sum("quantity").alias("total_sold")) \
    .select("rating", "total_sold")

# 6.3 Продукты с наибольшим количеством отзывов
most_reviewed = dim_product.orderBy(desc("reviews")).limit(10)

# ==================================================
# СОХРАНЕНИЕ В CLICKHOUSE
# ==================================================

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/reports"
ch_properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": ""
}

def save_to_clickhouse(df, table_name):
    df.write.jdbc(url=clickhouse_url, table=table_name, mode="overwrite", properties=ch_properties)
    print(f"Saved {table_name}")

# Сохраняем витрину 1
save_to_clickhouse(top_10_products, "top_10_products")
save_to_clickhouse(revenue_by_category, "revenue_by_category")
save_to_clickhouse(product_rating_reviews, "product_rating_reviews")

# Витрина 2
save_to_clickhouse(top_10_customers, "top_10_customers")
save_to_clickhouse(customers_by_country, "customers_by_country")
save_to_clickhouse(avg_check_per_customer, "avg_check_per_customer")

# Витрина 3
save_to_clickhouse(monthly_trends, "monthly_trends")
save_to_clickhouse(yearly_revenue, "yearly_revenue")
save_to_clickhouse(avg_order_by_month, "avg_order_by_month")

# Витрина 4
save_to_clickhouse(top_5_stores, "top_5_stores")
save_to_clickhouse(sales_by_city_country, "sales_by_city_country")
save_to_clickhouse(avg_check_by_store, "avg_check_by_store")

# Витрина 5
save_to_clickhouse(top_5_suppliers, "top_5_suppliers")
save_to_clickhouse(avg_price_by_supplier, "avg_price_by_supplier")
save_to_clickhouse(sales_by_supplier_country, "sales_by_supplier_country")

# Витрина 6
save_to_clickhouse(highest_rated, "highest_rated_products")
save_to_clickhouse(lowest_rated, "lowest_rated_products")
save_to_clickhouse(correlation_data, "rating_sales_correlation")
save_to_clickhouse(most_reviewed, "most_reviewed_products")

spark.stop()
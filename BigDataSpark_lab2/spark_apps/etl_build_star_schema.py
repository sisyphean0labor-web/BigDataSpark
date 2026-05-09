import os
import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, desc, row_number, year, month, dayofmonth, dayofweek, quarter, date_format, when
from pyspark.sql.window import Window

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/jovyan/work/postgresql-42.7.3.jar pyspark-shell'
spark = SparkSession.builder.appName("ETL_Build_Star_Schema").getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/bds_lab"
pg_properties = {"user": "admin", "password": "admin123", "driver": "org.postgresql.Driver"}

print("=" * 60)
print("1. Чтение mock_data_raw из PostgreSQL...")
raw_df = spark.read.jdbc(url=jdbc_url, table="mock_data_raw", properties=pg_properties)
print(f"   Прочитано {raw_df.count()} строк")

print("\n2. Создание измерений...")

# dim_customer
dim_customer = raw_df.select(
    "customer_email", "customer_first_name", "customer_last_name",
    "customer_age", "customer_country", "customer_postal_code",
    "customer_pet_type", "customer_pet_name", "customer_pet_breed"
).distinct()
dim_customer = dim_customer.withColumnRenamed("customer_email", "email")
dim_customer = dim_customer.withColumn("customer_id", row_number().over(Window.orderBy("email")))
print(f"   dim_customer: {dim_customer.count()}")

# dim_seller
dim_seller = raw_df.select(
    "seller_email", "seller_first_name", "seller_last_name",
    "seller_country", "seller_postal_code"
).distinct()
dim_seller = dim_seller.withColumnRenamed("seller_email", "email")
dim_seller = dim_seller.withColumn("seller_id", row_number().over(Window.orderBy("email")))
print(f"   dim_seller: {dim_seller.count()}")

# dim_product
dim_product = raw_df.select(
    "product_name", "product_brand", "product_category", "product_price",
    "product_weight", "product_color", "product_size", "product_material",
    "product_rating", "product_reviews", "product_release_date", "product_expiry_date",
    "pet_category", "product_description"
).distinct()
dim_product = dim_product.withColumn("product_id", row_number().over(Window.orderBy("product_name", "product_brand")))
print(f"   dim_product: {dim_product.count()}")

# dim_store
dim_store = raw_df.select(
    "store_name", "store_email", "store_location", "store_city",
    "store_state", "store_country", "store_phone"
).distinct()
dim_store = dim_store.withColumn("store_id", row_number().over(Window.orderBy("store_name", "store_email")))
print(f"   dim_store: {dim_store.count()}")

# dim_supplier
dim_supplier = raw_df.select(
    "supplier_name", "supplier_contact", "supplier_email",
    "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
).distinct()
dim_supplier = dim_supplier.withColumn("supplier_id", row_number().over(Window.orderBy("supplier_name")))
print(f"   dim_supplier: {dim_supplier.count()}")

# dim_date
dates_df = raw_df.select("sale_date").union(raw_df.select("product_release_date")) \
    .union(raw_df.select("product_expiry_date")).distinct()
dates_df = dates_df.withColumnRenamed("sale_date", "full_date")
dim_date = dates_df.withColumn("date_id",
    year("full_date") * 10000 + month("full_date") * 100 + dayofmonth("full_date"))
dim_date = dim_date.withColumn("year", year("full_date"))
dim_date = dim_date.withColumn("quarter", quarter("full_date"))
dim_date = dim_date.withColumn("month", month("full_date"))
dim_date = dim_date.withColumn("month_name", date_format("full_date", "MMMM"))
dim_date = dim_date.withColumn("day", dayofmonth("full_date"))
dim_date = dim_date.withColumn("day_of_week", dayofweek("full_date"))
dim_date = dim_date.withColumn("day_name", date_format("full_date", "EEEE"))
dim_date = dim_date.withColumn("is_weekend",
    when((dayofweek("full_date") == 1) | (dayofweek("full_date") == 7), True).otherwise(False))
print(f"   dim_date: {dim_date.count()}")

print("\n3. Создание fact_sales...")
fact_prep = raw_df.select(
    "id", "sale_quantity", "sale_total_price", "sale_date",
    col("customer_email").alias("customer_email"),
    col("seller_email").alias("seller_email"),
    "product_name", "product_brand",
    "store_name", "store_email",
    "supplier_name"
)

fact_sales = fact_prep.join(dim_customer, fact_prep.customer_email == dim_customer.email, how="left") \
    .join(dim_seller, fact_prep.seller_email == dim_seller.email, how="left") \
    .join(dim_product, on=["product_name", "product_brand"], how="left") \
    .join(dim_store, on=["store_name", "store_email"], how="left") \
    .join(dim_supplier, fact_prep.supplier_name == dim_supplier.supplier_name, how="left") \
    .join(dim_date, fact_prep.sale_date == dim_date.full_date, how="left")

fact_sales = fact_sales.select(
    col("id").alias("source_id"),
    "customer_id", "seller_id", "product_id", "store_id", "supplier_id", "date_id",
    "sale_quantity", "sale_total_price"
)
print(f"   fact_sales: {fact_sales.count()}")

print("\n4. Запись в PostgreSQL...")
dim_customer.write.jdbc(url=jdbc_url, table="dim_customer", mode="overwrite", properties=pg_properties)
dim_seller.write.jdbc(url=jdbc_url, table="dim_seller", mode="overwrite", properties=pg_properties)
dim_product.write.jdbc(url=jdbc_url, table="dim_product", mode="overwrite", properties=pg_properties)
dim_store.write.jdbc(url=jdbc_url, table="dim_store", mode="overwrite", properties=pg_properties)
dim_supplier.write.jdbc(url=jdbc_url, table="dim_supplier", mode="overwrite", properties=pg_properties)
dim_date.write.jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=pg_properties)
fact_sales.write.jdbc(url=jdbc_url, table="fact_sales", mode="overwrite", properties=pg_properties)

print("\n✅ Готово! Схема звезда построена через Spark.")
spark.stop()
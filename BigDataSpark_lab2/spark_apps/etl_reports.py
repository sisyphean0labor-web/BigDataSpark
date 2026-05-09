import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, desc

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/jovyan/work/postgresql-42.7.3.jar pyspark-shell'
spark = SparkSession.builder.appName("BDS_Lab2_Reports").getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/bds_lab"
pg_properties = {"user": "admin", "password": "admin123", "driver": "org.postgresql.Driver"}

print("📖 Чтение данных из PostgreSQL...")
fact_sales = spark.read.jdbc(url=jdbc_url, table="fact_sales", properties=pg_properties)
dim_product = spark.read.jdbc(url=jdbc_url, table="dim_product", properties=pg_properties)
dim_customer = spark.read.jdbc(url=jdbc_url, table="dim_customer", properties=pg_properties)
dim_date = spark.read.jdbc(url=jdbc_url, table="dim_date", properties=pg_properties)
dim_store = spark.read.jdbc(url=jdbc_url, table="dim_store", properties=pg_properties)
dim_supplier = spark.read.jdbc(url=jdbc_url, table="dim_supplier", properties=pg_properties)

print(f"✅ Данные загружены: fact_sales = {fact_sales.count()}")

sales_with_product = fact_sales.join(dim_product, "product_id")
sales_with_customer = fact_sales.join(dim_customer, "customer_id")
sales_with_date = fact_sales.join(dim_date, "date_id")
sales_with_store = fact_sales.join(dim_store, "store_id")
sales_with_supplier = fact_sales.join(dim_supplier, "supplier_id")

def save_csv(df, name):
    df.write.mode("overwrite").csv(f"/home/jovyan/work/reports_csv/{name}", header=True)
    print(f"✅ {name} сохранён в CSV")

# ============================================================
print("\n🏭 Витрина 1: Продажи по продуктам")
top_10_products = sales_with_product.groupBy("product_name", "product_brand") \
    .agg(_sum("sale_total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(10)
save_csv(top_10_products, "top_10_products")

revenue_by_category = sales_with_product.groupBy("product_category") \
    .agg(_sum("sale_total_price").alias("total_revenue"))
save_csv(revenue_by_category, "revenue_by_category")

product_rating_reviews = dim_product.select("product_name", "product_brand", "product_rating", "product_reviews")
save_csv(product_rating_reviews, "product_rating_reviews")

# ============================================================
print("\n👥 Витрина 2: Продажи по клиентам")
top_10_customers = sales_with_customer.groupBy("customer_id", "customer_first_name", "customer_last_name", "email") \
    .agg(_sum("sale_total_price").alias("total_spent")) \
    .orderBy(desc("total_spent")) \
    .limit(10)
save_csv(top_10_customers, "top_10_customers")

customers_by_country = dim_customer.groupBy("customer_country") \
    .agg(count("*").alias("customer_count"))
save_csv(customers_by_country, "customers_by_country")

avg_check_per_customer = sales_with_customer.groupBy("customer_id", "customer_first_name", "customer_last_name") \
    .agg(avg("sale_total_price").alias("avg_check"))
save_csv(avg_check_per_customer, "avg_check_per_customer")

# ============================================================
print("\n📅 Витрина 3: Продажи по времени")
monthly_trends = sales_with_date.groupBy("year", "month", "month_name") \
    .agg(_sum("sale_total_price").alias("monthly_revenue")) \
    .orderBy("year", "month")
save_csv(monthly_trends, "monthly_trends")

yearly_revenue = sales_with_date.groupBy("year") \
    .agg(_sum("sale_total_price").alias("yearly_revenue"))
save_csv(yearly_revenue, "yearly_revenue")

avg_order_by_month = sales_with_date.groupBy("year", "month", "month_name") \
    .agg(avg("sale_total_price").alias("avg_order_value")) \
    .orderBy("year", "month")
save_csv(avg_order_by_month, "avg_order_by_month")

# ============================================================
print("\n🏪 Витрина 4: Продажи по магазинам")
top_5_stores = sales_with_store.groupBy("store_name", "store_city", "store_country") \
    .agg(_sum("sale_total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(5)
save_csv(top_5_stores, "top_5_stores")

sales_by_city_country = sales_with_store.groupBy("store_country", "store_city") \
    .agg(_sum("sale_total_price").alias("revenue"), count("*").alias("transactions"))
save_csv(sales_by_city_country, "sales_by_city_country")

avg_check_by_store = sales_with_store.groupBy("store_name") \
    .agg(avg("sale_total_price").alias("avg_check"))
save_csv(avg_check_by_store, "avg_check_by_store")

# ============================================================
print("\n🏭 Витрина 5: Продажи по поставщикам")
top_5_suppliers = sales_with_supplier.groupBy("supplier_name") \
    .agg(_sum("sale_total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(5)
save_csv(top_5_suppliers, "top_5_suppliers")

sales_by_supplier_country = sales_with_supplier.groupBy("supplier_country") \
    .agg(_sum("sale_total_price").alias("revenue"))
save_csv(sales_by_supplier_country, "sales_by_supplier_country")

# ============================================================
print("\n⭐ Витрина 6: Качество продукции")
highest_rated = dim_product.orderBy(desc("product_rating")).limit(5)
save_csv(highest_rated, "highest_rated_products")

lowest_rated = dim_product.orderBy("product_rating").limit(5)
save_csv(lowest_rated, "lowest_rated_products")

correlation_data = sales_with_product.groupBy("product_rating") \
    .agg(_sum("sale_quantity").alias("total_sold")) \
    .orderBy("product_rating")
save_csv(correlation_data, "rating_sales_correlation")

most_reviewed = dim_product.orderBy(desc("product_reviews")).limit(10)
save_csv(most_reviewed, "most_reviewed_products")

print("\n🎉 Все отчёты сохранены в CSV!")
spark.stop()
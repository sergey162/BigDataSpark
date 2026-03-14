from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, to_date

print("Запуск Job 1: Чтение CSV -> Снежинка в PostgreSQL")

spark = SparkSession.builder \
    .appName("ETL_to_PostgreSQL") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

PG_URL = "jdbc:postgresql://postgres_db:5432/petshop_db"
PG_PROPS = {"user": "admin", "password": "adminpassword", "driver": "org.postgresql.Driver"}

print("Чтение данных из CSV (с поддержкой переносов строк)...")

df_raw = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .csv("/home/jovyan/work/data/mock_data_*.csv")


df_raw = df_raw.withColumn("sale_date", to_date("sale_date", "M/d/yyyy"))

print("Формирование схемы Снежинка...")

dim_suppliers = df_raw.select(
    "supplier_name", "supplier_contact", "supplier_email", 
    "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
).dropDuplicates(["supplier_name"]).filter("supplier_name IS NOT NULL") \
 .withColumn("supplier_id", monotonically_increasing_id())

dim_stores = df_raw.select(
    "store_name", "store_location", "store_city", "store_state", 
    "store_country", "store_phone", "store_email"
).dropDuplicates(["store_name"]).filter("store_name IS NOT NULL") \
 .withColumn("store_id", monotonically_increasing_id())

dim_customers = df_raw.select(
    "sale_customer_id", "customer_first_name", "customer_last_name", "customer_age", 
    "customer_email", "customer_country", "customer_postal_code", "customer_pet_type", 
    "customer_pet_name", "customer_pet_breed"
).dropDuplicates(["sale_customer_id"]).filter("sale_customer_id IS NOT NULL") \
 .withColumnRenamed("sale_customer_id", "customer_id")

dim_sellers = df_raw.select(
    "sale_seller_id", "seller_first_name", "seller_last_name", "seller_email", 
    "seller_country", "seller_postal_code"
).dropDuplicates(["sale_seller_id"]).filter("sale_seller_id IS NOT NULL") \
 .withColumnRenamed("sale_seller_id", "seller_id")

dim_products = df_raw.select(
    "sale_product_id", "product_name", "product_category", "pet_category", 
    "product_price", "product_weight", "product_color", "product_size", 
    "product_brand", "product_material", "product_description", "product_rating", 
    "product_reviews", "supplier_name"
).dropDuplicates(["sale_product_id"]).filter("sale_product_id IS NOT NULL") \
 .join(dim_suppliers.select("supplier_name", "supplier_id"), on="supplier_name", how="left") \
 .drop("supplier_name") \
 .withColumnRenamed("sale_product_id", "product_id")

fact_sales = df_raw.select(
    "sale_customer_id", "sale_seller_id", "sale_product_id", "sale_date", 
    "sale_quantity", "sale_total_price", "store_name"
).join(dim_stores.select("store_name", "store_id"), on="store_name", how="left") \
 .drop("store_name") \
 .withColumnRenamed("sale_customer_id", "customer_id") \
 .withColumnRenamed("sale_seller_id", "seller_id") \
 .withColumnRenamed("sale_product_id", "product_id") \
 .withColumn("sale_id", monotonically_increasing_id())

print("Запись таблиц в PostgreSQL...")
tables = {
    "dim_suppliers": dim_suppliers,
    "dim_stores": dim_stores,
    "dim_customers": dim_customers,
    "dim_sellers": dim_sellers,
    "dim_products": dim_products,
    "fact_sales": fact_sales
}

for table_name, df in tables.items():
    df.write.jdbc(url=PG_URL, table=table_name, mode="overwrite", properties=PG_PROPS)
    print(f"Таблица {table_name} загружена.")

print("Job 1 успешно завершен!")
spark.stop()
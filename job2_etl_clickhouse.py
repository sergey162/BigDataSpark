from pyspark.sql import SparkSession

print("Запуск Job 2: PostgreSQL -> Витрины в ClickHouse")

spark = SparkSession.builder \
    .appName("ETL_to_ClickHouse") \
    .getOrCreate()

PG_URL = "jdbc:postgresql://postgres_db:5432/petshop_db"
PG_PROPS = {
    "user": "admin", 
    "password": "adminpassword", 
    "driver": "org.postgresql.Driver"
}

CH_URL = "jdbc:clickhouse://spark-clickhouse:8123/reports_db"
CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "admin",
    "password": "adminpassword",
    "compress": "false"
}

print("Загрузка Снежинки из PostgreSQL в память Spark...")
tables = ["fact_sales", "dim_products", "dim_customers", "dim_stores", "dim_suppliers"]
for table in tables:
    df = spark.read.jdbc(PG_URL, table, properties=PG_PROPS)
    df.createOrReplaceTempView(table)

print("Генерация 6 витрин (отчетов)...")
reports = {}

reports["report_products"] = spark.sql("""
    SELECT p.product_category, p.product_name, SUM(f.sale_quantity) as total_sold, 
           SUM(f.sale_total_price) as total_revenue, AVG(p.product_rating) as avg_rating, 
           MAX(p.product_reviews) as total_reviews
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    GROUP BY p.product_category, p.product_name
""")

reports["report_customers"] = spark.sql("""
    SELECT c.customer_country, c.customer_first_name, c.customer_last_name, 
           SUM(f.sale_total_price) as total_spent, AVG(f.sale_total_price) as avg_check
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    GROUP BY c.customer_country, c.customer_first_name, c.customer_last_name
""")

reports["report_time"] = spark.sql("""
    SELECT year(f.sale_date) as sale_year, month(f.sale_date) as sale_month, 
           SUM(f.sale_total_price) as monthly_revenue, AVG(f.sale_total_price) as avg_order_size
    FROM fact_sales f
    GROUP BY sale_year, sale_month
""")

reports["report_stores"] = spark.sql("""
    SELECT s.store_name, s.store_country, s.store_city, 
           SUM(f.sale_total_price) as store_revenue, AVG(f.sale_total_price) as store_avg_check
    FROM fact_sales f
    JOIN dim_stores s ON f.store_id = s.store_id
    GROUP BY s.store_name, s.store_country, s.store_city
""")

reports["report_suppliers"] = spark.sql("""
    SELECT sup.supplier_name, sup.supplier_country, SUM(f.sale_total_price) as supplier_revenue, 
           AVG(p.product_price) as avg_product_price
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    JOIN dim_suppliers sup ON p.supplier_id = sup.supplier_id
    GROUP BY sup.supplier_name, sup.supplier_country
""")

reports["report_quality"] = spark.sql("""
    SELECT product_name, product_category, product_rating, product_reviews, product_price
    FROM dim_products
""")

schemas = {
    "report_products": "product_category STRING, product_name STRING, total_sold BIGINT, total_revenue DOUBLE, avg_rating DOUBLE, total_reviews BIGINT",
    "report_customers": "customer_country STRING, customer_first_name STRING, customer_last_name STRING, total_spent DOUBLE, avg_check DOUBLE",
    "report_time": "sale_year INT, sale_month INT, monthly_revenue DOUBLE, avg_order_size DOUBLE",
    "report_stores": "store_name STRING, store_country STRING, store_city STRING, store_revenue DOUBLE, store_avg_check DOUBLE",
    "report_suppliers": "supplier_name STRING, supplier_country STRING, supplier_revenue DOUBLE, avg_product_price DOUBLE",
    "report_quality": "product_name STRING, product_category STRING, product_rating DOUBLE, product_reviews BIGINT, product_price DOUBLE"
}

print("Запись витрин в ClickHouse...")
for report_name, df_report in reports.items():
    
    df_clean = df_report.fillna(0).fillna("Unknown")

    df_clean.write.jdbc(url=CH_URL, table=report_name, mode="append", properties=CH_PROPS)
    print(f"Витрина {report_name} загружена.")

print("Job 2 успешно завершен!")
spark.stop()
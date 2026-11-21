from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_TABLE = "lakehouse.silver.products"
GOLD_TABLE = "lakehouse.gold.product_category_summary"


def build_spark():
    return (
        SparkSession.builder
        .appName("ucv-query-iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config(
            "spark.sql.catalog.lakehouse.warehouse",
            "s3a://ucv-data-platform/products"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://floci:4566")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .getOrCreate()
    )


spark = build_spark()

silver = spark.table(SILVER_TABLE)
gold = spark.table(GOLD_TABLE)

print("\n=== SILVER ===")
silver.printSchema()
silver.show(20, truncate=False)
print("Silver rows:", silver.count())

print("\n=== GOLD ===")
gold.printSchema()
gold.show(50, truncate=False)
print("Gold rows:", gold.count())

print("\n=== VALIDACIONES ===")
print("Silver product_id nulos:",
      silver.filter(F.col("product_id").isNull()).count())
print("Silver price <= 0:",
      silver.filter(F.col("price") <= 0).count())
print("Silver duplicados por product_id:",
      silver.groupBy("product_id").count().filter("count > 1").count())
print("Gold category nula:",
      gold.filter(F.col("category").isNull()).count())
print("Gold product_count <= 0:",
      gold.filter(F.col("product_count") <= 0).count())
print("Gold duplicados por category:",
      gold.groupBy("category").count().filter("count > 1").count())

spark.stop()
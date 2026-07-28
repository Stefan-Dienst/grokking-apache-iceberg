from pyspark.sql import SparkSession

from iceberg.config import CATALOG_URI, WAREHOUSE_PATH

# Create SparkSession with JDBC catalog pointing to SQLite
spark = (
    SparkSession.builder.appName("IcebergWithSQLiteCatalog")
    # Iceberg packages
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.0,"
        "org.xerial:sqlite-jdbc:3.46.0.0",
    )  # SQLite JDBC driver
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Configure catalog to use JDBC (SQLite)
    .config("spark.sql.catalog.marvel", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.marvel.type", "jdbc")
    .config("spark.sql.catalog.marvel.uri", CATALOG_URI)
    .config("spark.sql.catalog.marvel.warehouse", f"file://{WAREHOUSE_PATH}")
    .config("spark.sql.catalog.marvel.jdbc.useUnicode", "true")
    .config("spark.sql.catalog.marvel.jdbc.verifyServerCertificate", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Attempting to connect to SQLite catalog...")
print(f"Catalog URI: {CATALOG_URI}")
print("=" * 80)

# Try to list namespaces
namespaces = spark.sql("SHOW NAMESPACES IN marvel").collect()
print("\nNamespaces in catalog:")
for ns in namespaces:
    print(f"  - {ns.namespace}")

# Load the table
print("\n" + "=" * 80)
print("Loading table marvel.xmen.characters")
print("=" * 80)

df = spark.sql("SELECT * FROM marvel.xmen.characters")
df.show(truncate=False)

print(f"\nTotal records: {df.count()}")

print("\n" + "=" * 80)
print("SUCCESS: PySpark is using your SQLite catalog!")
print("=" * 80)

spark.stop()

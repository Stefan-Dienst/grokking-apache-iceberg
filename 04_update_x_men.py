from pyspark.sql import SparkSession

from iceberg.config import DATA_CATALOG_DB, WAREHOUSE_PATH

# Create SparkSession with JDBC catalog pointing to SQLite
spark = (
    SparkSession.builder.appName("IcebergWithSQLiteCatalog")
    # Iceberg packages
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
        "org.xerial:sqlite-jdbc:3.46.0.0",
    )  # SQLite JDBC driver
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Configure catalog to use JDBC (SQLite)
    .config("spark.sql.catalog.marvel", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.marvel.type", "jdbc")
    .config(
        "spark.sql.catalog.marvel.uri",
        f"jdbc:sqlite:///{WAREHOUSE_PATH}/{DATA_CATALOG_DB}",
    )
    .config("spark.sql.catalog.marvel.warehouse", f"file://{WAREHOUSE_PATH}")
    .config("spark.sql.catalog.marvel.jdbc.useUnicode", "true")
    .config("spark.sql.catalog.marvel.jdbc.verifyServerCertificate", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# List namespaces
namespaces = spark.sql("SHOW NAMESPACES IN marvel").collect()
print("\nNamespaces in catalog:")
for ns in namespaces:
    print(f"  - {ns.namespace}")

# Show current state of the table
print("\nCurrent table state:")
df = spark.sql("SELECT * FROM marvel.xmen.characters ORDER BY id")
df.show(truncate=False)

# Adjust again to merge on write.
print("\n" + "=" * 80)
print("Setting table properties for merge-on-read updates")
print("=" * 80)

spark.sql("""
    ALTER TABLE marvel.xmen.characters 
    SET TBLPROPERTIES (
        'write.update.mode' = 'merge-on-read',
        'format-version' = '2'
    )
""")

# Verify the properties
df_props = spark.sql("SHOW TBLPROPERTIES marvel.xmen.characters")
print("\nTable properties:")
df_props.show(truncate=False)

# Cyclops has decided to stop being an x-men. We will change his active status.
print("\n" + "=" * 80)
print("Updating Cyclops (id=1): Setting active = false")
print("=" * 80)

spark.sql("""
    UPDATE marvel.xmen.characters
    SET active = false
    WHERE id = 1
""")

# Check the result
print("\nTable after update:")
df = spark.sql("SELECT * FROM marvel.xmen.characters ORDER BY id")
df.show(truncate=False)

print(f"\nTotal records: {df.count()}")

# What happened?
# A delete file that deletes the initial row for Cyclops was created + a data file that holds his
# new active status. In addition a manifest file was created for each of the new data/delete files.
# The latest snapshot now points to all existing manifest files, which in combindation show the
# desired state of the table.

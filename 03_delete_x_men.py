from pyspark.sql import SparkSession

from iceberg.config import DATA_CATALOG_DB, WAREHOUSE_PATH

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

# Try to list namespaces
namespaces = spark.sql("SHOW NAMESPACES IN marvel").collect()
print("\nNamespaces in catalog:")
for ns in namespaces:
    print(f"  - {ns.namespace}")

# Load the table
print("\n" + "=" * 80)
print("Loading table marvel.xmen.characters")
print("=" * 80)

# Now delete Cyclopse by id
spark.sql("""
    DELETE FROM marvel.xmen.characters
    WHERE id = 1
""")

# What happened?
# TODO: Show files.
# Instead of writing a delete file like promised spark rewrote one data file without cyclopses record in it.
# This is because per default spark is setup to use copy on write TODO: verify this.

# To actually force spark to create a delete file we need to set its mode to merge-on-read.
spark.sql("""ALTER TABLE marvel.xmen.characters SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read'
    )
         """)

df = spark.sql("SHOW TBLPROPERTIES marvel.xmen.characters")
df.show()


# Now because Cyclopse left, Jean Grey leaves to ... delete her record.
spark.sql("""
    DELETE FROM marvel.xmen.characters
    WHERE id = 2
""")

# Check if delete went through
df = spark.sql("SELECT * FROM marvel.xmen.characters")
df.show(truncate=False)

print(f"\nTotal records: {df.count()}")

# Tada now we see a delete file:
# $ parquet-tools show 00191-4-c72fc53f-f3ec-4a18-bbfe-088754a26cab-00001-deletes.parquet
# +-------------------------------------------------------------------------------------------------------+-------+
# | file_path                                                                                             |   pos |
# |-------------------------------------------------------------------------------------------------------+-------|
# | file:/tmp/warehouse/xmen/characters/data/00026-5-c6974f66-aec1-4c01-a2d2-5b35887bda4b-0-00001.parquet |     0 |
# +-------------------------------------------------------------------------------------------------------+-------+

# Now let's not delete single rows but delete on some condition.
spark.sql("""
    DELETE FROM marvel.xmen.characters
    WHERE active = false
""")

# Check if delete went through
df = spark.sql("SELECT * FROM marvel.xmen.characters")
df.show(truncate=False)

print(f"\nTotal records: {df.count()}")

# Note here that we only get delete files and no delete vectors because we are using iceberg spec 2,
# see format-version 2.

spark.stop()

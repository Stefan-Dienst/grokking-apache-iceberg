import gc
import os
import shutil
import time

from pyarrow.csv import read_csv
from pyiceberg.catalog import load_catalog
from pyspark.sql import SparkSession

from iceberg.config import DATA_CATALOG_DB, WAREHOUSE_PATH


def setup_warehouse():
    if os.path.exists(WAREHOUSE_PATH):
        shutil.rmtree(WAREHOUSE_PATH)
    os.mkdir(WAREHOUSE_PATH)


def clear_files():
    table_path = os.path.join(WAREHOUSE_PATH, "xmen", "characters")
    data_path = os.path.join(table_path, "data")

    if os.path.exists(data_path):
        shutil.rmtree(data_path)

    metadata_path = os.path.join(table_path, "metadata")
    if os.path.exists(metadata_path):
        shutil.rmtree(metadata_path)


def load_sqlite_catalog():
    return load_catalog(
        "marvel",  # Name of the catalog. One can store multiple catalogs in the same database.
        **{
            "type": "sql",  # We will use the SQLCatalog type.
            "uri": f"sqlite:///{WAREHOUSE_PATH}/{DATA_CATALOG_DB}",  # Gives the location of the sqlite database.
            "warehouse": f"file://{WAREHOUSE_PATH}",  # Give the path were the metadata and data of the actual tables will be stored.
        },
    )


def create_marvel_xmen_namespace(catalog):
    catalog.create_namespace_if_not_exists(
        "xmen",
        properties={
            "description": "Mutant superheroes with powers",
            "owner": "professor_x",
            "location": f"file://{WAREHOUSE_PATH}/xmen",
        },
    )


def setup_base_table():
    setup_warehouse()
    catalog = load_sqlite_catalog()
    create_marvel_xmen_namespace(catalog)

    # Create table
    df = read_csv("./x-men.csv")

    table = catalog.create_table(
        identifier="xmen.characters",
        schema=df.schema,
    )

    # Add data
    table.append(df)


def recreate_base_table_with_spark(spark):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv("./x-men.csv")
    )
    df.writeTo("marvel.xmen.characters").create()

    return spark


def connect_with_spark():
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
    return spark

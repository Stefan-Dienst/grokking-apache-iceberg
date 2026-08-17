# We have now seen that iceberg brings great features that systematically improve on weakness of Hive tables.
# But, as always, there is no free lunch.
# As we have seen in the previous sections, to make all these features possible, Iceberg tables must do a lot of heavy lifting.
# Every change of data, be it an append, delete or update, requrires multiple meta data files to be written in addition to the actual data files.
# Also, depending on what write mode is used, much redundant data may be written and stored indefinitely.
# Down the line, just normally using Iceberg tables can lead to shortcomings like wasted storage, reading many small data files or cumbersome scanning through many metadata files.
# To combat this Iceberg supplies a set of maintenance operations.

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

# The first thing we can do is reduce the number of metadata files.
# As stated before, one of the big advantage of Iceberg is that it reduces the number of API calls needed for planning a scan by containing multiple file paths in a single manifest file.
# But if we store a single manifest file for each data file this advantage is lost.
# To avoid this we can rewrite manifest files using:
spark.sql(""" CALL marvel.system.rewrite_manifests('xmen.characters') """).show()

# +-------------------------+---------------------+
# |rewritten_manifests_count|added_manifests_count|
# +-------------------------+---------------------+
# |                        3|                    1|
# +-------------------------+---------------------+

# Which leaves us with a single manifest file that references all data files.


# The second thing we can do is to combine multiple small data files into bigger ones, a technique called compaction.
# While this does not matter for our toy data set example anyways, for production environments one usually aims for data files of around 512 MB (the default target data file size when compacting).
# This number is not choosen arbitrarily, but is a sweet spot when balancing multiple opposing incentives.
# On one side large files are favored, because:
# - Each data size contains necessary metadta information, i.e. headers and footers. The large the data file the better (lower) the storage amplification. This means one writes more data the one actually wants to writes, i.e. row groups, compared to what one needs to write, i.e. headers/footers.
# - As the data files are usually accessed via a network the API call overhead per file can add up. (Same issue as with scan planning). Having bigger and therefore less files reduces this overhead.
# On the other side small files are favored, because:
# - Each file can be worked on in parallel by a worker of the query engine used. With more smaller files the workload can be better parallelized.
# - As files are typically processed by workers in memory the content of a file should fit inside the memory of a worker. Due to typical resource allocation in a query engine cluster this poses a limit on how big a file should get.
# Considering these factors, data files around 512 MB have just proven themselves to be efficient in practice for common applications.

# To actually compact our data files we can call the following:
spark.sql(
    """ CALL marvel.system.rewrite_data_files(table => 'xmen.characters', options => map('rewrite-all', 'true')) """
).show()

# +--------------------------+----------------------+---------------------+-----------------------+--------------------------+
# |rewritten_data_files_count|added_data_files_count|rewritten_bytes_count|failed_data_files_count|removed_delete_files_count|
# +--------------------------+----------------------+---------------------+-----------------------+--------------------------+
# |                         3|                     1|                 8131|                      0|                         0|
# +--------------------------+----------------------+---------------------+-----------------------+--------------------------+

# Which leaves us with a single data file, which also simplifies the latest manifest file.

# The last maintenance step I want to show is the expiration of snapshots.
# As almost every operation on an iceberg table creates a new snapshots many manifest list files accumlate over time.
# Depending on the frequency and granularity with which one appends or deletes data from an Iceberg table, keeping all these snaphosts may not be needed and just waste storage.
# Therefore one can simply expire old snapshots and automatically get rid of data and manifest files that are only references in the expired snapshots.

# Before:

# /tmp/warehouse
# $ tree
# .
# ├── pyiceberg_catalog.db
# └── xmen
#     └── characters
#         ├── data
#         │   ├── 00000-0-30c48a46-c190-41cb-9c89-8861bdd6a4b3.parquet
#         │   ├── 00000-0-89d409ea-cf3c-43fa-b048-0bc187186e60.parquet
#         │   ├── 00000-0-a769853a-a2db-4287-bfd1-f7c89d66d26b.parquet
#         │   └── 00000-5-eb8d5a9d-a6d6-48df-ab22-3387a3426791-0-00001.parquet
#         └── metadata
#             ├── 00000-5e0db55d-0b0e-4743-93e9-a704ca28f15c.metadata.json
#             ├── 00001-5d658b87-2061-4a1c-a189-97411f322da7.metadata.json
#             ├── 00002-9b18134f-937d-4692-b3e7-43d938f34725.metadata.json
#             ├── 00003-26766ff5-c31f-4f9e-b622-7d1c278eb435.metadata.json
#             ├── 00004-f6e6c26f-cded-4dd3-98b5-f83ed37d8605.metadata.json
#             ├── 00005-5f680a92-4b16-4b8d-913d-813ec9c3b3b4.metadata.json
#             ├── 00006-f36b55fe-722a-4c3e-bbfa-d5da5381a1eb.metadata.json
#             ├── 00007-afa571c1-0d96-4557-9b00-ec90f59befa1.metadata.json
#             ├── 00008-a2a55eab-57fe-4b5f-8696-ed4be108706c.metadata.json
#             ├── 30c48a46-c190-41cb-9c89-8861bdd6a4b3-m0.avro
#             ├── 89d409ea-cf3c-43fa-b048-0bc187186e60-m0.avro
#             ├── 9b48bfae-8e1a-4a1a-81ea-61990e186e8b-m0.avro
#             ├── 9b48bfae-8e1a-4a1a-81ea-61990e186e8b-m1.avro
#             ├── a769853a-a2db-4287-bfd1-f7c89d66d26b-m0.avro
#             ├── optimized-m-463ec091-f9ac-4a0f-8a3a-e65617563e55.avro
#             ├── snap-3014518774835702005-1-ed8e4d58-ffd8-4c28-addf-9708aa31759c.avro
#             ├── snap-4956052972558252728-0-a769853a-a2db-4287-bfd1-f7c89d66d26b.avro
#             ├── snap-768964918616401465-1-9b48bfae-8e1a-4a1a-81ea-61990e186e8b.avro
#             ├── snap-8380523983098070968-0-89d409ea-cf3c-43fa-b048-0bc187186e60.avro
#             └── snap-9164814029224031324-0-30c48a46-c190-41cb-9c89-8861bdd6a4b3.avro

# Here we expire all snapshots older than a future date (so all), but want retain atleast one.
spark.sql(
    """ CALL marvel.system.expire_snapshots('xmen.characters', TIMESTAMP '2026-08-18 00:00:00.000', 1); """
).show()

# +------------------------+-----------------------------------+-----------------------------------+----------------------------+----------------------------+------------------------------+
# |deleted_data_files_count|deleted_position_delete_files_count|deleted_equality_delete_files_count|deleted_manifest_files_count|deleted_manifest_lists_count|deleted_statistics_files_count|
# +------------------------+-----------------------------------+-----------------------------------+----------------------------+----------------------------+------------------------------+
# |                       0|                                  0|                                  0|                           1|                           2|                             0|
# +------------------------+-----------------------------------+-----------------------------------+----------------------------+----------------------------+------------------------------+

# /tmp/warehouse
# $ tree
# .
# ├── pyiceberg_catalog.db
# └── xmen
#     └── characters
#         ├── data
#         │   ├── 00000-0-30c48a46-c190-41cb-9c89-8861bdd6a4b3.parquet
#         │   ├── 00000-0-89d409ea-cf3c-43fa-b048-0bc187186e60.parquet
#         │   ├── 00000-0-a769853a-a2db-4287-bfd1-f7c89d66d26b.parquet
#         │   └── 00000-5-eb8d5a9d-a6d6-48df-ab22-3387a3426791-0-00001.parquet
#         └── metadata
#             ├── 00000-5e0db55d-0b0e-4743-93e9-a704ca28f15c.metadata.json
#             ├── 00001-5d658b87-2061-4a1c-a189-97411f322da7.metadata.json
#             ├── 00002-9b18134f-937d-4692-b3e7-43d938f34725.metadata.json
#             ├── 00003-26766ff5-c31f-4f9e-b622-7d1c278eb435.metadata.json
#             ├── 00004-f6e6c26f-cded-4dd3-98b5-f83ed37d8605.metadata.json
#             ├── 00005-5f680a92-4b16-4b8d-913d-813ec9c3b3b4.metadata.json
#             ├── 00006-f36b55fe-722a-4c3e-bbfa-d5da5381a1eb.metadata.json
#             ├── 00007-afa571c1-0d96-4557-9b00-ec90f59befa1.metadata.json
#             ├── 00008-a2a55eab-57fe-4b5f-8696-ed4be108706c.metadata.json
#             ├── 00009-5505814b-5f37-4b34-b54d-a4fa9ef364d1.metadata.json
#             ├── 30c48a46-c190-41cb-9c89-8861bdd6a4b3-m0.avro
#             ├── 89d409ea-cf3c-43fa-b048-0bc187186e60-m0.avro
#             ├── 9b48bfae-8e1a-4a1a-81ea-61990e186e8b-m0.avro
#             ├── 9b48bfae-8e1a-4a1a-81ea-61990e186e8b-m1.avro
#             ├── a769853a-a2db-4287-bfd1-f7c89d66d26b-m0.avro
#             ├── snap-4956052972558252728-0-a769853a-a2db-4287-bfd1-f7c89d66d26b.avro
#             ├── snap-768964918616401465-1-9b48bfae-8e1a-4a1a-81ea-61990e186e8b.avro
#             └── snap-8380523983098070968-0-89d409ea-cf3c-43fa-b048-0bc187186e60.avro

# Here we see that actually three snapshots still remain in our iceberg table.
# This is because snapshots with are referenced by tag or branch, in our case the tag `v1` and the branch `dev`, are protected from expiration.

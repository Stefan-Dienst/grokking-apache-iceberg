from pyspark.sql import SparkSession

from iceberg.config import DATA_CATALOG_DB, WAREHOUSE_PATH
from iceberg.setup import (
    connect_with_spark,
    recreate_base_table_with_spark,
    setup_base_table,
)

setup_base_table()

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

# Try to list namespaces
namespaces = spark.sql("SHOW NAMESPACES IN marvel").collect()
print("\nNamespaces in catalog:")
for ns in namespaces:
    print(f"{ns.namespace}")

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
# Show files in graph.
# (Note here that the data file naming convention has changed as we are using pyspark here, which differs from pyiceberg: https://iceberg.apache.org/javadoc/0.11.0/org/apache/iceberg/io/OutputFileFactory.html)
# Instead of writing a delete file like promised spark rewrote one data file without cyclopses record in it.
# This is because per default the write.delete.mode is set to copy-on-write, see here: https://iceberg.apache.org/docs/latest/configuration/#write-properties

# New metadata file
# {
#  "format-version" : 2,
#  "table-uuid" : "18f3b09b-e372-4485-9fc1-9efb0d707ced",
#  "location" : "file:///tmp/warehouse/xmen/characters",
#  "last-sequence-number" : 3,
#  "last-updated-ms" : 1785960290085,
#  "last-column-id" : 6,
#  "current-schema-id" : 0,
#  "schemas" : [ {
#    "type" : "struct",
#    "schema-id" : 0,
#    "fields" : [ {
#      "id" : 1,
#      "name" : "id",
#      "required" : false,
#      "type" : "long"
#    }, {
#      "id" : 2,
#      "name" : "name",
#      "required" : false,
#      "type" : "string"
#    }, {
#      "id" : 3,
#      "name" : "alias",
#      "required" : false,
#      "type" : "string"
#    }, {
#      "id" : 4,
#      "name" : "powers",
#      "required" : false,
#      "type" : "string"
#    }, {
#      "id" : 5,
#      "name" : "birth_year",
#      "required" : false,
#      "type" : "long"
#    }, {
#      "id" : 6,
#      "name" : "active",
#      "required" : false,
#      "type" : "boolean"
#    } ]
#  } ],
#  "default-spec-id" : 0,
#  "partition-specs" : [ {
#    "spec-id" : 0,
#    "fields" : [ ]
#  } ],
#  "last-partition-id" : 999,
#  "default-sort-order-id" : 0,
#  "sort-orders" : [ {
#    "order-id" : 0,
#    "fields" : [ ]
#  } ],
#  "properties" : { },
#  "current-snapshot-id" : 805341815771319914,
#  "refs" : {
#    "main" : {
#      "snapshot-id" : 805341815771319914,
#      "type" : "branch"
#    }
#  },
#  "snapshots" : [ {
#    "sequence-number" : 1,
#    "snapshot-id" : 6272734660913065949,
#    "timestamp-ms" : 1785959855556,
#    "summary" : {
#      "operation" : "append",
#      "added-files-size" : "2846",
#      "added-data-files" : "1",
#      "added-records" : "10",
#      "total-data-files" : "1",
#      "total-delete-files" : "0",
#      "total-records" : "10",
#      "total-files-size" : "2846",
#      "total-position-deletes" : "0",
#      "total-equality-deletes" : "0"
#    },
#    "manifest-list" : "file:///tmp/warehouse/xmen/characters/metadata/snap-6272734660913065949-0-9e1b2748-a8e5-4b09-b74f-a9f78e7d2524.avro",
#    "schema-id" : 0
#  }, {
#    "sequence-number" : 2,
#    "snapshot-id" : 1589152794795701372,
#    "parent-snapshot-id" : 6272734660913065949,
#    "timestamp-ms" : 1785959869920,
#    "summary" : {
#      "operation" : "append",
#      "added-files-size" : "2571",
#      "added-data-files" : "1",
#      "added-records" : "3",
#      "total-data-files" : "2",
#      "total-delete-files" : "0",
#      "total-records" : "13",
#      "total-files-size" : "5417",
#      "total-position-deletes" : "0",
#      "total-equality-deletes" : "0"
#    },
#    "manifest-list" : "file:///tmp/warehouse/xmen/characters/metadata/snap-1589152794795701372-0-650f881a-a243-44be-a619-71d98da23730.avro",
#    "schema-id" : 0
#  }, {
#    "sequence-number" : 3,
#    "snapshot-id" : 805341815771319914,
#    "parent-snapshot-id" : 1589152794795701372,
#    "timestamp-ms" : 1785960290085,
#    "summary" : {
#      "operation" : "overwrite",
#      "spark.app.id" : "local-1785960282910",
#      "added-data-files" : "1",
#      "deleted-data-files" : "1",
#      "added-records" : "9",
#      "deleted-records" : "10",
#      "added-files-size" : "2198",
#      "removed-files-size" : "2846",
#      "changed-partition-count" : "1",
#      "total-records" : "12",
#      "total-files-size" : "4769",
#      "total-data-files" : "2",
#      "total-delete-files" : "0",
#      "total-position-deletes" : "0",
#      "total-equality-deletes" : "0"
#    },
#    "manifest-list" : "file:/tmp/warehouse/xmen/characters/metadata/snap-805341815771319914-1-6083779d-ac4d-4ae2-af4b-7c212f0f273d.avro",
#    "schema-id" : 0
#  } ],
#  "statistics" : [ ],
#  "partition-statistics" : [ ],
#  "snapshot-log" : [ {
#    "timestamp-ms" : 1785959855556,
#    "snapshot-id" : 6272734660913065949
#  }, {
#    "timestamp-ms" : 1785959869920,
#    "snapshot-id" : 1589152794795701372
#  }, {
#    "timestamp-ms" : 1785960290085,
#    "snapshot-id" : 805341815771319914
#  } ],
#  "metadata-log" : [ {
#    "timestamp-ms" : 1785959855499,
#    "metadata-file" : "file:///tmp/warehouse/xmen/characters/metadata/00000-c7f9d101-0e74-4d1d-99b6-0c832a67ca0e.metadata.json"
#  }, {
#    "timestamp-ms" : 1785959855556,
#    "metadata-file" : "file:///tmp/warehouse/xmen/characters/metadata/00001-54766511-b75d-4f7f-b113-e910ed0bf889.metadata.json"
#  }, {
#    "timestamp-ms" : 1785959869920,
#    "metadata-file" : "file:///tmp/warehouse/xmen/characters/metadata/00002-250a384e-624e-4b19-8ab9-7c41ea17d280.metadata.json"
#  } ]
# }

# Manifest list

# {
#   "manifest_path": "file:/tmp/warehouse/xmen/characters/metadata/6083779d-ac4d-4ae2-af4b-7c212f0f273d-m1.avro",
#   "manifest_length": 7024,
#   "partition_spec_id": 0,
#   "content": 0,
#   "sequence_number": 3,
#   "min_sequence_number": 3,
#   "added_snapshot_id": 805341815771319914,
#   "added_files_count": 1,
#   "existing_files_count": 0,
#   "deleted_files_count": 0,
#   "added_rows_count": 9,
#   "existing_rows_count": 0,
#   "deleted_rows_count": 0,
#   "partitions": {
#     "array": []
#   }
# }
# {
#   "manifest_path": "file:///tmp/warehouse/xmen/characters/metadata/650f881a-a243-44be-a619-71d98da23730-m0.avro",
#   "manifest_length": 4661,
#   "partition_spec_id": 0,
#   "content": 0,
#   "sequence_number": 2,
#   "min_sequence_number": 2,
#   "added_snapshot_id": 1589152794795701372,
#   "added_files_count": 1,
#   "existing_files_count": 0,
#   "deleted_files_count": 0,
#   "added_rows_count": 3,
#   "existing_rows_count": 0,
#   "deleted_rows_count": 0,
#   "partitions": {
#     "array": []
#   }
# }
# {
#   "manifest_path": "file:/tmp/warehouse/xmen/characters/metadata/6083779d-ac4d-4ae2-af4b-7c212f0f273d-m0.avro",
#   "manifest_length": 7016,
#   "partition_spec_id": 0,
#   "content": 0,
#   "sequence_number": 3,
#   "min_sequence_number": 3,
#   "added_snapshot_id": 805341815771319914,
#   "added_files_count": 0,
#   "existing_files_count": 0,
#   "deleted_files_count": 1,
#   "added_rows_count": 0,
#   "existing_rows_count": 0,
#   "deleted_rows_count": 10,
#   "partitions": {
#     "array": []
#   }
# }

# Noter here that the new manifest list actually points to three manigests.
# One from our uuid-2 operation, which just contains the 3 newly added x-men, which are still part of the table.
# Second the newly written manifest that points to the old uuid-1 data file, where we added the first 10 x-men.
# This one is now marked as deleted, so it will not be considered for the current state of the table.
# And third, the newly written manifest that points to the uuid-3 data file that contains the first 10 x-men minus Cyclopse, hence 9 remaining x-men.
# In total the table now consists of two data files and has 12 x-men.

# In the spirit of upcoming timetravel, let's rewind our last operation and change the mode for wrtiting deletes from copy-on-write, to merge-on-read.
recreate_base_table_with_spark(spark)

# This can be done via
spark.sql("""ALTER TABLE marvel.xmen.characters SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read'
    )
         """)

df = spark.sql("SHOW TBLPROPERTIES marvel.xmen.characters")
df.show()

# +-------------------+-------------------+
# |                key|              value|
# +-------------------+-------------------+
# |current-snapshot-id|5279280090035835473|
# |             format|    iceberg/parquet|
# |     format-version|                  2|
# |  write.delete.mode|      merge-on-read|
# +-------------------+-------------------+


# Now let's delete Cyclopse again.
spark.sql("""
    DELETE FROM marvel.xmen.characters
    WHERE id = 1
""")

# Check if delete went through
df = spark.sql("SELECT * FROM marvel.xmen.characters")
df.show(truncate=False)

print(f"\nTotal records: {df.count()}")

# Now several things have happend.
# First, by changing the `write.delete.mode` we created a new snapshot which reflects this change

# "properties" : {
#   "write.delete.mode" : "merge-on-read"
# },

# Second, by running the delete we have created a delete file.
# The current snapshot now points to three 3 manifest files, which point to their corresponding data/delete file.
# When the table is now read first the data files are read.
# Then the delete file is checked, which shows the following:

# $ parquet-tools show 00191-1-<uuid-3>-0001-deletes.parquet
# +-----------------------------------------------------------------------------+-------+
# | file_path                                                                  |   pos |
# |----------------------------------------------------------------------------+-------|
# | file:/tmp/warehouse/xmen/characters/data/00000-0-<uuid-1>.parquet.parquet |     0 |
# +--------------------------------------------------------------------------+-------+

# This tells the reader to remove the record at postition 0 from the file `00000-0-<uuid-1>.parquet.parquet`, i.e. cyclopse.

# Now let's again reverse time to cover a different aspect of Apache Iceberg.
recreate_base_table_with_spark(spark)
# As previousely stated the core of Apache Iceberg is the specification, but there are different version of this specification.
# When writing this blog post v1, v2 and v3 have been released, while v4 is in active developement.
# But here care is to be taken, because not every implementation supports all versions of the spec.
# For example we initally created our table with pyiceberg, which does only supports v2 yet, while in pyspark we can use v3.
# Let's just do this and change the table format using:
spark.sql("""
    ALTER TABLE marvel.xmen.characters 
    SET TBLPROPERTIES ('format-version' = '3',     
                       'write.delete.mode' = 'merge-on-read'
)
""")

# Now let's delete Cyclopse again.
spark.sql("""
    DELETE FROM marvel.xmen.characters
    WHERE id = 1
""")

# Check if delete went through
df = spark.sql("SELECT * FROM marvel.xmen.characters")
df.show(truncate=False)

print(f"\nTotal records: {df.count()}")

# The file setup is now identical to the previous v2 delete, but we got a puffin delete file instead of a parquet one.
# This is because v3 switched from delete files to represent a delete to deletion vectors.
# The big difference here is that a delete file stores one line per deleted rows, like above, and there can be many delete files associated with a single data file.
# For example if we would delete more and more x-men one by one, we would create a new delete file for each deletion.
# This adds more and more work for a reader, having to scan many delete files, leading to bad performances.
# In contrary a deletion vector is bitmap that encodes what rows are deleted, i.e. an array of bits, one for each row of the associated data file, where a 1 indicates that this record is deleted.
# Now the v3 specification states that writes have to make sure that only one deletion vector exists for any data file.
# Hence, a deleted is now no longer a file creation operation, but a bitmap modification operation.
# For more infos on this see [here](https://iceberglakehouse.com/posts/iceberg-v3-deletion-vectors-merge-on-read/).

# Let's have a look at the puffin file, which is in very short a storing blobs and a JSON, that describes what how to interpret these blobs.
# In our case the JSON looks like this:
# {
#   "blobs": [
#     {
#       "type": "deletion-vector-v1",
#       "fields": [
#         2147483645
#       ],
#       "snapshot-id": -1,
#       "sequence-number": -1,
#       "offset": 4,
#       "length": 42,
#       "properties": {
#         "referenced-data-file": "file:///tmp/warehouse/xmen/characters/data/00000-0-4d456502-ebfc-4f22-b185-72cd34a1defa.parquet",
#         "cardinality": "1"
#       }
#     }
#   ],
#   "properties": {
#     "created-by": "Apache Iceberg 1.10.0 (commit 2114bf631e49af532d66e2ce148ee49dd1dd1f1f)"
#   }
# }

# This tell us that the blob at offset 4 is a deletion-vector-v1 type, which refernces the data file 00000-0-<uuid-2>.parquet and deletes a single row (cardinality 1).
# To now actually know which row was deleted, i.e. checking which bit is 1 in the bitmap, a reader would now need to decode the blob.


spark.stop()

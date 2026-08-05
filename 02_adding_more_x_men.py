from pyiceberg.catalog import load_catalog

from iceberg.config import DATA_CATALOG_DB, WAREHOUSE_PATH

warehouse_path = "/tmp/warehouse"
catalog = load_catalog(
    "marvel",  # Name of the catalog. One can store multiple catalogs in the same database.
    **{
        "type": "sql",  # We will use the SQLCatalog type.
        "uri": f"sqlite:///{WAREHOUSE_PATH}/{DATA_CATALOG_DB}",  # Gives the location of the sqlite database.
        "warehouse": f"file://{WAREHOUSE_PATH}",  # Give the path were the metadata and data of the actual tables will be stored.
    },
)

from pyarrow.csv import read_csv

# Now let's add three more rows to our table and see what happens.
df = read_csv("./x-men2.csv")


table = catalog.load_table(identifier="xmen.characters")

table.append(df)
print(len(table.scan().to_arrow()))

# In the catalog the table now points to a new metadata file:

# sqlite> select * from iceberg_tables;
# catalog_name  table_namespace  table_name  metadata_location                                             previous_metadata_location
# ------------  ---------------  ----------  ------------------------------------------------------------  ------------------------------------------------------------
# marvel        xmen             characters  file:///tmp/warehouse/xmen/characters/metadata/00002-d2c158b  file:///tmp/warehouse/xmen/characters/metadata/00001-4812401
#                                            2-71f4-4160-9e3d-e4cb9e6f94ff.metadata.json                   4-4717-4700-9263-d1f15b7a6206.metadata.json

# We have created:
# - 1 new data file
# - 1 new metadata file
# - 1 new snap (manifest list) file
# - 1 new manifest file

# /tmp/warehouse
# $ tree
# .
# ├── pyiceberg_catalog.db
# └── xmen
#     └── characters
#         ├── data
#         │   ├── 00000-0-a83404b3-5060-4b69-8164-4d730c07b4d5.parquet
#         │   └── 00000-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.parquet
#         └── metadata
#             ├── 00000-689a8442-64c6-4152-8444-4713cfb4d4bf.metadata.json
#             ├── 00001-48124014-4717-4700-9263-d1f15b7a6206.metadata.json
#             ├── 00002-d2c158b2-71f4-4160-9e3d-e4cb9e6f94ff.metadata.json
#             ├── a83404b3-5060-4b69-8164-4d730c07b4d5-m0.avro
#             ├── b4508cc3-e44a-474a-aab8-078f8c6aca01-m0.avro
#             ├── snap-1110035385487351968-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.avro
#             └── snap-3505164069080030858-0-a83404b3-5060-4b69-8164-4d730c07b4d5.avro

# 5 directories, 10 files


# So the catalog points to a new metadat file, which now contains multiple snapshots, but also states the current one:

# "current-snapshot-id": 3505164069080030858,
# "snapshots": [
#   {
#     "snapshot-id": 1110035385487351968,
#     "sequence-number": 1,
#     "timestamp-ms": 1754402715529,
#     "manifest-list": "file:///tmp/warehouse/xmen/characters/metadata/snap-1110035385487351968-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.avro",
#     "summary": {
#       "operation": "append",
#       "added-files-size": "2846",
#       "added-data-files": "1",
#       "added-records": "10",
#       "total-data-files": "1",
#       "total-delete-files": "0",
#       "total-records": "10",
#       "total-files-size": "2846",
#       "total-position-deletes": "0",
#       "total-equality-deletes": "0"
#     },
#     "schema-id": 0
#   },
#   {
#     "snapshot-id": 3505164069080030858,
#     "parent-snapshot-id": 1110035385487351968,
#     "sequence-number": 2,
#     "timestamp-ms": 1754405155465,
#     "manifest-list": "file:///tmp/warehouse/xmen/characters/metadata/snap-3505164069080030858-0-a83404b3-5060-4b69-8164-4d730c07b4d5.avro",
#     "summary": {
#       "operation": "append",
#       "added-files-size": "2571",
#       "added-data-files": "1",
#       "added-records": "3",
#       "total-data-files": "2",
#       "total-delete-files": "0",
#       "total-records": "13",
#       "total-files-size": "5417",
#       "total-position-deletes": "0",
#       "total-equality-deletes": "0"
#     },
#     "schema-id": 0
#   }
# ],

# When we look at the new manifest list file that the most recent snapshot points to we now actually see that it contains two manifest files:

# $ avro-tools tojson snap-3505164069080030858-0-a83404b3-5060-4b69-8164-4d730c07b4d5.avro | jq
# 25/08/05 16:50:07 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# {
#   "manifest_path": "file:///tmp/warehouse/xmen/characters/metadata/a83404b3-5060-4b69-8164-4d730c07b4d5-m0.avro",
#   "manifest_length": 4661,
#   "partition_spec_id": 0,
#   "content": 0,
#   "sequence_number": 2,
#   "min_sequence_number": 2,
#   "added_snapshot_id": 3505164069080030858,
#   "added_files_count": 1,
#   "existing_files_count": 0,
#   "deleted_files_count": 0,
#   "added_rows_count": 3,
#   "existing_rows_count": 0,
#   "deleted_rows_count": 0,
#   "partitions": {
#     "array": []
#   },
#   "key_metadata": null
# }
# {
#   "manifest_path": "file:///tmp/warehouse/xmen/characters/metadata/b4508cc3-e44a-474a-aab8-078f8c6aca01-m0.avro",
#   "manifest_length": 4655,
#   "partition_spec_id": 0,
#   "content": 0,
#   "sequence_number": 1,
#   "min_sequence_number": 1,
#   "added_snapshot_id": 1110035385487351968,
#   "added_files_count": 1,
#   "existing_files_count": 0,
#   "deleted_files_count": 0,
#   "added_rows_count": 10,
#   "existing_rows_count": 0,
#   "deleted_rows_count": 0,
#   "partitions": {
#     "array": []
#   },
#   "key_metadata": null
# }

# Where the second one is just the old manifest file and the new one points to the new parquet file.
# Just the same story.

# TODO: talk about statistics here maybe or later?

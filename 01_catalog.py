import os
import shutil

from pyiceberg.catalog import load_catalog

# Apache Iceberg tables needs to be registed in a catalog to be usable. We will be using the
# SQLCatalog, which is backed by sqlite and accessed via sqlalchemy.
from iceberg.config import DATA_CATALOG_DB, WAREHOUSE_PATH

if os.path.exists(WAREHOUSE_PATH):
    shutil.rmtree(WAREHOUSE_PATH)
os.mkdir(WAREHOUSE_PATH)

catalog = load_catalog(
    "marvel",  # Name of the catalog. One can store multiple catalogs in the same database.
    **{
        "type": "sql",  # We will use the SQLCatalog type.
        "uri": f"sqlite:///{WAREHOUSE_PATH}/{DATA_CATALOG_DB}",  # Gives the location of the sqlite database.
        "warehouse": f"file://{WAREHOUSE_PATH}",  # Give the path were the metadata and data of the actual tables will be stored.
    },
)

# This will create the sqlite database pyiceberg_catalog.db.
# You can access it using `sqlite3 ./pyiceberg_catalog.db`.
# Use `.header on` and `.mode column` before querying for a nicer output.

# $ sqlite3 pyiceberg_catalog.db
# SQLite version 3.45.1 2024-01-30 16:01:20
# Enter ".help" for usage hints.
# sqlite> .headers on
# sqlite> .mode column

# You should now see two empty tables using `.tables`:

# sqlite> .tables
# iceberg_namespace_properties  iceberg_tables


# We can now create a name space in this catalog.
# Namespace are used to hierarchically group tables and are useful to avoid name conflicts.
# One can also give the properties, which can be useful to e.g. give more information like
# description or owner, but can also be used to give a specific location where data of this
# namespace shall be stored.

# Let's create one namespace for the x-men
catalog.create_namespace_if_not_exists(
    "xmen",
    properties={
        "description": "Mutant superheroes with powers",
        "owner": "professor_x",
        "location": f"file://{WAREHOUSE_PATH}/xmen",
    },
)

# And another for "normal" mutants
catalog.create_namespace_if_not_exists(
    "mutants",
    properties={
        "description": "Information on mutants",
        "owner": "Senator Kelly",
        "location": f"file://{WAREHOUSE_PATH}/mutants",
    },
)

# We can see them now in the database:

# sqlite> select * from iceberg_namespace_properties;
# catalog_name  namespace  property_key  property_value
# ------------  ---------  ------------  ------------------------------
# marvel        xmen       description   Mutant superheroes with powers
# marvel        xmen       owner         professor_x
# marvel        xmen       location      file:///tmp/warehouse/xmen
# marvel        mutants    description   Information on mutants
# marvel        mutants    owner         Senator Kelly
# marvel        mutants    location      file:///tmp/warehouse/mutants

# But also who them via the catalog api
print(catalog.list_namespaces())
print(catalog.load_namespace_properties("xmen"))

# [('mutants',), ('xmen',)]
# {'description': 'Mutant superheroes with powers', 'location': 'file:///tmp/warehouse/xmen', 'owner': 'professor_x'}

# Now let's add data to our catalog.

# We will first load a csv of x-men characters as an arrow table.
from pyarrow.csv import read_csv

df = read_csv("./x-men.csv")

# And we can print the schema
print(df.schema)

# id: int64
# name: string
# alias: string
# powers: string
# birth_year: int64
# active: bool

# Before executing this the directory looks like this:

# /tmp/warehouse
# $ ls
# pyiceberg_catalog.db

table = catalog.create_table(
    identifier="xmen.characters",
    schema=df.schema,
)

# Afterwards we see this:

# /tmp/warehouse
# $ tree
# .
# ├── pyiceberg_catalog.db
# └── xmen
#     └── characters
#         └── metadata
#             └── 00000-689a8442-64c6-4152-8444-4713cfb4d4bf.metadata.json

# 4 directories, 2 files


# In our sqlite database we now also have a row in the `iceberg_tables` table:

# sqlite> select * from iceberg_tables;
# catalog_name  table_namespace  table_name  metadata_location                                             previous_metadata_location
# ------------  ---------------  ----------  ------------------------------------------------------------  --------------------------
# marvel        xmen             characters  file:///tmp/warehouse/xmen/characters/metadata/00000-f033b40
#                                            6-e2f7-43d1-b2bb-82e021f3ffdb.metadata.json

# When we look at this file we see the following:


# {
#   "location": "file:///tmp/warehouse/xmen/characters",
#   "table-uuid": "037f79f9-1e57-4a8e-bb25-f173924c3e3c",
#   "last-updated-ms": 1754402272360,
#   "last-column-id": 6,
#   "schemas": [
#     {
#       "type": "struct",
#       "fields": [
#         {
#           "id": 1,
#           "name": "id",
#           "type": "long",
#           "required": false
#         },
#         {
#           "id": 2,
#           "name": "name",
#           "type": "string",
#           "required": false
#         },
#         {
#           "id": 3,
#           "name": "alias",
#           "type": "string",
#           "required": false
#         },
#         {
#           "id": 4,
#           "name": "powers",
#           "type": "string",
#           "required": false
#         },
#         {
#           "id": 5,
#           "name": "birth_year",
#           "type": "long",
#           "required": false
#         },
#         {
#           "id": 6,
#           "name": "active",
#           "type": "boolean",
#           "required": false
#         }
#       ],
#       "schema-id": 0,
#       "identifier-field-ids": []
#     }
#   ],
#   "current-schema-id": 0,
#   "partition-specs": [
#     {
#       "spec-id": 0,
#       "fields": []
#     }
#   ],
#   "default-spec-id": 0,
#   "last-partition-id": 999,
#   "properties": {},
#   "snapshots": [],
#   "snapshot-log": [],
#   "metadata-log": [],
#   "sort-orders": [
#     {
#       "order-id": 0,
#       "fields": []
#     }
#   ],
#   "default-sort-order-id": 0,
#   "refs": {},
#   "statistics": [],
#   "format-version": 2,
#   "last-sequence-number": 0
# }

# We will go into details more.
# The most important thing is that the schema reflects the schema of our pyarrow table and that
# alot of information are stored about the table.

# Next we will actually add data to the table.
# For this we gonna simply append our pyarrow table.

table.append(df)
print(len(table.scan().to_arrow()))

# This changed the following:

# /tmp/warehouse
# $ tree
# .
# ├── pyiceberg_catalog.db
# └── xmen
#     └── characters
#         ├── data
#         │   └── 00000-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.parquet
#         └── metadata
#             ├── 00000-689a8442-64c6-4152-8444-4713cfb4d4bf.metadata.json
#             ├── 00001-48124014-4717-4700-9263-d1f15b7a6206.metadata.json
#             ├── b4508cc3-e44a-474a-aab8-078f8c6aca01-m0.avro
#             └── snap-1110035385487351968-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.avro


# And if we look at the sqlite database

# sqlite> select * from iceberg_tables;
# catalog_name  table_namespace  table_name  metadata_location                                             previous_metadata_location
# ------------  ---------------  ----------  ------------------------------------------------------------  ------------------------------------------------------------
# marvel        xmen             characters  file:///tmp/warehouse/xmen/characters/metadata/00001-4812401  file:///tmp/warehouse/xmen/characters/metadata/00000-689a844
#                                            4-4717-4700-9263-d1f15b7a6206.metadata.json                   2-64c6-4152-8444-4713cfb4d4bf.metadata.json

# We can see that the metadata_location now points toa new file file.
# (Notice the first 5 characters now show a 00001 instead of a 00000.)

# If we take a look at the new matadata file we see:

# $ diff 00000-689a8442-64c6-4152-8444-4713cfb4d4bf.metadata.json 00001-48124014-4717-4700-9263-d1f15b7a6206.metadata.json
# 4c4
# <   "last-updated-ms": 1754402715452,
# ---
# >   "last-updated-ms": 1754402715529,
# 61,63c61,94
# <   "snapshots": [],
# <   "snapshot-log": [],
# <   "metadata-log": [],
# ---
# >   "current-snapshot-id": 1110035385487351968,
# >   "snapshots": [
# >     {
# >       "snapshot-id": 1110035385487351968,
# >       "sequence-number": 1,
# >       "timestamp-ms": 1754402715529,
# >       "manifest-list": "file:///tmp/warehouse/xmen/characters/metadata/snap-1110035385487351968-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.avro",
# >       "summary": {
# >         "operation": "append",
# >         "added-files-size": "2846",
# >         "added-data-files": "1",
# >         "added-records": "10",
# >         "total-data-files": "1",
# >         "total-delete-files": "0",
# >         "total-records": "10",
# >         "total-files-size": "2846",
# >         "total-position-deletes": "0",
# >         "total-equality-deletes": "0"
# >       },
# >       "schema-id": 0
# >     }
# >   ],
# >   "snapshot-log": [
# >     {
# >       "snapshot-id": 1110035385487351968,
# >       "timestamp-ms": 1754402715529
# >     }
# >   ],
# >   "metadata-log": [
# >     {
# >       "metadata-file": "file:///tmp/warehouse/xmen/characters/metadata/00000-689a8442-64c6-4152-8444-4713cfb4d4bf.metadata.json",
# >       "timestamp-ms": 1754402715452
# >     }
# >   ],
# 71c102,107
# <   "refs": {},
# ---
# >   "refs": {
# >     "main": {
# >       "snapshot-id": 1110035385487351968,
# >       "type": "branch"
# >     }
# >   },
# 74c110
# <   "last-sequence-number": 0
# ---
# >   "last-sequence-number": 1

# So what changed?

# We have a different `last-updated-ms`, which makes sense.
# We now have a our first snapshots, we will go into detail of this.
# We have a ref TODO: don't know what this means.
# We have a different `last-sequence-number`, TODO: don't know what this means.

# Let's focus first on the snapshot.

# {
#     "snapshot-id": 1110035385487351968,
#     "sequence-number": 1,
#     "timestamp-ms": 1754402715529,
#     "manifest-list": "file:///tmp/warehouse/xmen/characters/metadata/snap-1110035385487351968-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.avro",
#     "summary": {
#         "operation": "append",
#         "added-files-size": "2846",
#         "added-data-files": "1",
#         "added-records": "10",
#         "total-data-files": "1",
#         "total-delete-files": "0",
#         "total-records": "10",
#         "total-files-size": "2846",
#         "total-position-deletes": "0",
#         "total-equality-deletes": "0",
#     },
#     "schema-id": 0,
# }

# In the end this identifies is the actual data behind out table, by pointing to a manifest-list.
# Also it gives us a lot of metadata on how this snapshot came to be, which may be useful.
# But let's first look into the manifest-list, for which we need to decode AVRO files.

# Insert:
# I use avro-tools.
# I have created a directory
# `mkdir tools`
# and downlaoded the jar there:
#

# ~/tools
# $ wget https://downloads.apache.org/avro/avro-1.11.3/java/avro-tools-1.11.3.jar

# Then I use the alias
# alias avro-tools='java -jar ~/tools/avro-tools-1.11.3.jar'

# Which allows me to just run
# avro-tools tojson snap-1110035385487351968-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.avro

# Coupled with jq i get a nice output:

# $ avro-tools tojson snap-1110035385487351968-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.avro | jq
# 25/08/05 16:27:27 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
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

# In addition to metadat info it points to a manifest file.

# Inspecting this we get:

# $ avro-tools tojson b4508cc3-e44a-474a-aab8-078f8c6aca01-m0.avro | jq
# 25/08/05 16:29:43 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# {
#   "status": 1,
#   "snapshot_id": {
#     "long": 1110035385487351968
#   },
#   "sequence_number": null,
#   "file_sequence_number": null,
#   "data_file": {
#     "content": 0,
#     "file_path": "file:///tmp/warehouse/xmen/characters/data/00000-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.parquet",
#     "file_format": "PARQUET",
#     "partition": {},
#     "record_count": 10,
#     "file_size_in_bytes": 2846,
#     "column_sizes": {
#       "array": [
#         {
#           "key": 1,
#           "value": 143
#         },
#         {
#           "key": 2,
#           "value": 222
#         },
#         {
#           "key": 3,
#           "value": 192
#         },
#         {
#           "key": 4,
#           "value": 330
#         },
#         {
#           "key": 5,
#           "value": 149
#         },
#         {
#           "key": 6,
#           "value": 50
#         }
#       ]
#     },
#     "value_counts": {
#       "array": [
#         {
#           "key": 1,
#           "value": 10
#         },
#         {
#           "key": 2,
#           "value": 10
#         },
#         {
#           "key": 3,
#           "value": 10
#         },
#         {
#           "key": 4,
#           "value": 10
#         },
#         {
#           "key": 5,
#           "value": 10
#         },
#         {
#           "key": 6,
#           "value": 10
#         }
#       ]
#     },
#     "null_value_counts": {
#       "array": [
#         {
#           "key": 1,
#           "value": 0
#         },
#         {
#           "key": 2,
#           "value": 0
#         },
#         {
#           "key": 3,
#           "value": 0
#         },
#         {
#           "key": 4,
#           "value": 0
#         },
#         {
#           "key": 5,
#           "value": 0
#         },
#         {
#           "key": 6,
#           "value": 0
#         }
#       ]
#     },
#     "nan_value_counts": {
#       "array": []
#     },
#     "lower_bounds": {
#       "array": [
#         {
#           "key": 1,
#           "value": "\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000"
#         },
#         {
#           "key": 2,
#           "value": "Anna Marie"
#         },
#         {
#           "key": 3,
#           "value": "Beast"
#         },
#         {
#           "key": 4,
#           "value": "Optic blasts, te"
#         },
#         {
#           "key": 5,
#           "value": "X\u0007\u0000\u0000\u0000\u0000\u0000\u0000"
#         },
#         {
#           "key": 6,
#           "value": "\u0000"
#         }
#       ]
#     },
#     "upper_bounds": {
#       "array": [
#         {
#           "key": 1,
#           "value": "\n\u0000\u0000\u0000\u0000\u0000\u0000\u0000"
#         },
#         {
#           "key": 2,
#           "value": "Scott Summers"
#         },
#         {
#           "key": 3,
#           "value": "Wolverine"
#         },
#         {
#           "key": 4,
#           "value": "Weather control-"
#         },
#         {
#           "key": 5,
#           "value": "Á\u0007\u0000\u0000\u0000\u0000\u0000\u0000"
#         },
#         {
#           "key": 6,
#           "value": "\u0001"
#         }
#       ]
#     },
#     "key_metadata": null,
#     "split_offsets": {
#       "array": [
#         4
#       ]
#     },
#     "equality_ids": null,
#     "sort_order_id": null
#   }
# }

# Which points us a parquet file, the actual data file.

# Using parquet tools to have alook we get:

# /tmp/warehouse/xmen/characters/data 31s
# $ parquet-tools show ./00000-0-b4508cc3-e44a-474a-aab8-078f8c6aca01.parquet
# +------+-----------------+--------------+-------------------------------------------+--------------+----------+
# |   id | name            | alias        | powers                                    |   birth_year | active   |
# |------+-----------------+--------------+-------------------------------------------+--------------+----------|
# |    1 | Scott Summers   | Cyclops      | Optic blasts, team leadership             |         1970 | True     |
# |    2 | Jean Grey       | Phoenix      | Telepathy, telekinesis, Phoenix Force     |         1972 | True     |
# |    3 | Logan           | Wolverine    | Regeneration, adamantium claws            |         1880 | True     |
# |    4 | Ororo Munroe    | Storm        | Weather control, flight                   |         1975 | True     |
# |    5 | Hank McCoy      | Beast        | Super strength, agility, genius intellect |         1968 | False    |
# |    6 | Kurt Wagner     | Nightcrawler | Teleportation, wall crawling              |         1978 | True     |
# |    7 | Pietro Maximoff | Quicksilver  | Super speed                               |         1980 | False    |
# |    8 | Charles Xavier  | Professor X  | Telepathy, mind control                   |         1940 | False    |
# |    9 | Kitty Pryde     | Shadowcat    | Phasing through objects                   |         1985 | True     |
# |   10 | Anna Marie      | Rogue        | Power absorption                          |         1979 | True     |
# +------+-----------------+--------------+-------------------------------------------+--------------+----------+

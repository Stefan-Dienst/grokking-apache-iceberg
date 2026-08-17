import os
import shutil

from pyarrow.csv import read_csv
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids, pyarrow_to_schema
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.transforms import IdentityTransform, TruncateTransform

# Apache Iceberg tables needs to be registed in a catalog to be usable. We will be using the
# SQLCatalog, which is backed by sqlite and accessed via sqlalchemy.
from iceberg.config import DATA_CATALOG_DB, WAREHOUSE_PATH
from iceberg.setup import (
    create_marvel_xmen_namespace,
    load_sqlite_catalog,
    setup_warehouse,
)

setup_warehouse()

catalog = load_sqlite_catalog()
create_marvel_xmen_namespace(catalog)

df = read_csv("./x-men4.csv")

print(df.schema)

# id: int64
# name: string
# alias: string
# powers: string
# birth_year: int64
# active: bool


schema_without_ids = _pyarrow_to_schema_without_ids(df.schema)
schema = assign_fresh_schema_ids(schema_without_ids)

print(schema)


# In the old hive way partitions were solely encoded in the directory path, e.g. `/year=2026/month=08/day=13`.
# This goes by the name of hive style partitioning.
# While it is straight forward, there are two downsides to it.
# The first is that data values are directly used in the path.
# For special characters like spaces or slashes this could lead to errors depending on the storage.

# Iceberg handles this nicley by [URL encoding special characters](https://github.com/apache/iceberg/pull/10329).
# Let's look at this for some x-men with special characters in their aliases:


# id,name,alias,powers,birth_year,active
# 1,Warren Kenneth Worthington III,Angel/Archangel,"Flight with feathered wings, aerial combat",1963,TRUE
# 2,Kevin Sydney,Changeling=Morph,"Shapeshifting, Psionic powers, Skilled actor",1968,FALSE

# Here we will just partition on the value of the alias field, without changing it, i.e. we use an identity transformation:
partition_spec = PartitionSpec(
    PartitionField(
        source_id=3, field_id=1000, transform=IdentityTransform(), name="alias"
    )
)

table = catalog.create_table(
    identifier="xmen.characters",
    schema=schema,
    partition_spec=partition_spec,
)

table.append(df)
print(len(table.scan().to_arrow()))

# This yields:

# $ tree
# .
# ├── pyiceberg_catalog.db
# └── xmen
#     └── characters
#         ├── data
#         │   ├── alias=Angel%2FArchangel
#         │   │   └── 00000-0-e5aec275-6efa-4d5f-a7ac-72533466b1eb.parquet
#         │   └── alias=Changeling%3DMorph
#         │       └── 00000-1-e5aec275-6efa-4d5f-a7ac-72533466b1eb.parquet
#         └── metadata
#             ├── 00000-48ee9325-cc23-4c4b-954d-3f26c6ef92ba.metadata.json
#             ├── 00001-1fea52fa-35e0-4ef2-94ab-98e1dab8c321.metadata.json
#             ├── e5aec275-6efa-4d5f-a7ac-72533466b1eb-m0.avro
#             └── snap-5353218680111079023-0-e5aec275-6efa-4d5f-a7ac-72533466b1eb.avro

# The special characters are safley encoded: `/` -> `%2F` and `=` -> `%3D`, which is a nice saftey.

# But the bigger feature is that Iceberg actually decouples the partitioning of a table from its physical layout.
# When we created the table the information of the partition spec is stored in the metadata file:
#
# "partition-specs": [
#     {
#       "spec-id": 0,
#       "fields": [
#         {
#           "source-id": 3,
#           "field-id": 1000,
#           "transform": "identity",
#           "name": "alias"
#         }
#       ]
#     }
#   ]

# If we now come to the conclusion that using the full alias for partitioning our x-men table may not be that smart, we can just change it.
# For example we could delete our identity transformation and partition by the first letter of the alias by using the `TruncateTransform`:
with table.update_spec() as update:
    update.remove_field("alias")
    update.add_field("alias", TruncateTransform(1), "alias_truncated")

# For other transformations see [the spec here](https://iceberg.apache.org/spec/#partitioning).

# I we now append more x-men...

df = read_csv("./x-men.csv")
table.append(df)

# $ tree
# .
# ├── pyiceberg_catalog.db
# └── xmen
#     └── characters
#         ├── data
#         │   ├── alias=Angel%2FArchangel
#         │   │   └── 00000-0-b3d7744e-faaf-4f7e-9c26-6990749d5ba6.parquet
#         │   ├── alias=Changeling%3DMorph
#         │   │   └── 00000-1-b3d7744e-faaf-4f7e-9c26-6990749d5ba6.parquet
#         │   ├── alias_truncated=B
#         │   │   └── 00000-4-b5aae724-9662-49cc-8adc-2ebc8337993d.parquet
#         │   ├── alias_truncated=C
#         │   │   └── 00000-0-b5aae724-9662-49cc-8adc-2ebc8337993d.parquet
#         │   ├── alias_truncated=N
#         │   │   └── 00000-5-b5aae724-9662-49cc-8adc-2ebc8337993d.parquet
#         │   ├── alias_truncated=P
#         │   │   └── 00000-1-b5aae724-9662-49cc-8adc-2ebc8337993d.parquet
#         │   ├── alias_truncated=Q
#         │   │   └── 00000-6-b5aae724-9662-49cc-8adc-2ebc8337993d.parquet
#         │   ├── alias_truncated=R
#         │   │   └── 00000-7-b5aae724-9662-49cc-8adc-2ebc8337993d.parquet
#         │   ├── alias_truncated=S
#         │   │   └── 00000-3-b5aae724-9662-49cc-8adc-2ebc8337993d.parquet
#         │   └── alias_truncated=W
#         │       └── 00000-2-b5aae724-9662-49cc-8adc-2ebc8337993d.parquet
#         └── metadata
#             ├── 00000-9287a6d5-405e-42c2-82c2-20cb9a3c443f.metadata.json
#             ├── 00001-3e498caa-acb8-4766-9a3d-2485ddb94363.metadata.json
#             ├── 00002-a9ed9991-a447-4206-8a87-0308f8627106.metadata.json
#             ├── 00003-ef0ae0d4-cd1a-472c-bce5-0782f4a20cdf.metadata.json
#             ├── b3d7744e-faaf-4f7e-9c26-6990749d5ba6-m0.avro
#             ├── b5aae724-9662-49cc-8adc-2ebc8337993d-m0.avro
#             ├── snap-2952667899251160202-0-b5aae724-9662-49cc-8adc-2ebc8337993d.avro
#             └── snap-4796657928117529314-0-b3d7744e-faaf-4f7e-9c26-6990749d5ba6.avro


# ... we see that the old partitions are still there but for the new records the new partition spec was used.
# The most recent metadata file now contains a new partition spec

# "partition-specs": [
#     {
#       "spec-id": 0,
#       "fields": [
#         {
#           "source-id": 3,
#           "field-id": 1000,
#           "transform": "identity",
#           "name": "alias"
#         }
#       ]
#     },
#     {
#       "spec-id": 1,
#       "fields": [
#         {
#           "source-id": 3,
#           "field-id": 1001,
#           "transform": "truncate[1]",
#           "name": "alias_truncated"
#         }
#       ]
#     }
#   ]

# and the manifest list (snap) states what partition spec was used to create which manifest file.

# $ avro-tools tojson ./xmen/characters/metadata/snap-2952667899251160202-0-b5aae724-9662-49cc-8adc-2ebc8337993d.avro| jq
# 26/08/13 23:02:33 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# {
#   "manifest_path": "file:///tmp/warehouse/xmen/characters/metadata/b5aae724-9662-49cc-8adc-2ebc8337993d-m0.avro",
#   "manifest_length": 7325,
#   "partition_spec_id": 1,
#   "content": 0,
#   "sequence_number": 2,
#   "min_sequence_number": 2,
#   "added_snapshot_id": 2952667899251160202,
#   "added_files_count": 8,
#   "existing_files_count": 0,
#   "deleted_files_count": 0,
#   "added_rows_count": 10,
#   "existing_rows_count": 0,
#   "deleted_rows_count": 0,
#   "partitions": {
#     "array": [
#       {
#         "contains_null": false,
#         "contains_nan": {
#           "boolean": false
#         },
#         "lower_bound": {
#           "bytes": "B"
#         },
#         "upper_bound": {
#           "bytes": "W"
#         }
#       }
#     ]
#   },
#   "key_metadata": null
# }
# {
#   "manifest_path": "file:///tmp/warehouse/xmen/characters/metadata/b3d7744e-faaf-4f7e-9c26-6990749d5ba6-m0.avro",
#   "manifest_length": 5268,
#   "partition_spec_id": 0,
#   "content": 0,
#   "sequence_number": 1,
#   "min_sequence_number": 1,
#   "added_snapshot_id": 4796657928117529314,
#   "added_files_count": 2,
#   "existing_files_count": 0,
#   "deleted_files_count": 0,
#   "added_rows_count": 2,
#   "existing_rows_count": 0,
#   "deleted_rows_count": 0,
#   "partitions": {
#     "array": [
#       {
#         "contains_null": false,
#         "contains_nan": {
#           "boolean": false
#         },
#         "lower_bound": {
#           "bytes": "Angel/Archangel"
#         },
#         "upper_bound": {
#           "bytes": "Changeling=Morph"
#         }
#       }
#     ]
#   },
#   "key_metadata": null
# }

# The concept of decoupling logical from physical layout is similiar to the previous schema evolution section.
# In short it allows a reader to still understand the "old way" of partitioning our data, while a writer can write new data with the new spec.
# To show this we can look at what happens if we want to fitler for x-men, whos alias starts with a "C":
scan = table.scan(row_filter="alias like 'C%'")

# Then we can look at the files that reader identified:
tasks = scan.plan_files()

print("\n" + "=" * 80)
print("Files to read:")
print("=" * 80)

for task in tasks:
    print(f"\nFile: {task.file.file_path}")
    print(f"  Partition: {task.file.partition}")
    print(f"  Record count: {task.file.record_count}")
    print(f"  File size: {task.file.file_size_in_bytes} bytes")
    if hasattr(task.file, "spec_id"):
        print(f"  Spec ID: {task.file.spec_id}")

# Hence the reader understood both partitioning spec and found the correct files to read.

results = scan.to_pandas()
print("\n" + "=" * 80)
print("Query Results:")
print("=" * 80)
print(results)

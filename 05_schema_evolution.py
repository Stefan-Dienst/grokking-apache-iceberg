from pyarrow.csv import read_csv
from pyiceberg.catalog import load_catalog, load_sql
from pyiceberg.types import StringType

from iceberg.config import DATA_CATALOG_DB, WAREHOUSE_PATH
from iceberg.setup import load_sqlite_catalog, setup_base_table

setup_base_table()


catalog = load_sqlite_catalog()
table = catalog.load_table("xmen.characters")
# Show current schema
print(table.schema())
# table {
#   1: id: optional long
#   2: name: optional string
#   3: alias: optional string
#   4: powers: optional string
#   5: birth_year: optional long
#   6: active: optional boolean
# }

# Change the schema by renaming a column, adding a new one and changing the order.
with table.update_schema() as update:
    update.rename_column("alias", "codename")
    update.add_column("first_appearance", StringType())
    update.move_after("active", "first_appearance")


# # Show new schema with re-named column
# print(table.schema)
# table {
#   1: id: optional long
#   2: name: optional string
#   3: codename: optional string
#   4: powers: optional string
#   5: birth_year: optional long
#   6: active: optional boolean
#   7: first_appearance: optional string
# }


# # Now let's add two more x-men that follow the new schema.
df = read_csv("./x-men3.csv")
table.append(df)

# Show new look of table.
print(table.scan().to_pandas())
#   id              name      codename                                      powers  birth_year         first_appearance  active
#   15  Illyana Rasputin         Magik  Teleportation through Limbo and dark magic        1982      Giant-Size X-Men #1    True
#   18  Roberto da Costa       Sunspot  Solar energy absorption and super strength        1984  Marvel Graphic Novel #4    True
#    1     Scott Summers       Cyclops               Optic blasts, team leadership        1970                      NaN    True
#    2         Jean Grey       Phoenix       Telepathy, telekinesis, Phoenix Force        1972                      NaN    True
#    3             Logan     Wolverine              Regeneration, adamantium claws        1880                      NaN    True
#    4      Ororo Munroe         Storm                     Weather control, flight        1975                      NaN    True
#    5        Hank McCoy         Beast   Super strength, agility, genius intellect        1968                      NaN   False
#    6       Kurt Wagner  Nightcrawler                Teleportation, wall crawling        1978                      NaN    True
#    7   Pietro Maximoff   Quicksilver                                 Super speed        1980                      NaN   False
#    8    Charles Xavier   Professor X                     Telepathy, mind control        1940                      NaN   False
#    9       Kitty Pryde     Shadowcat                     Phasing through objects        1985                      NaN    True
#   10        Anna Marie         Rogue                            Power absorption        1979                      NaN    True

# Notice that we see the schema as changed and the for the new column, that was none existing for the old data we wrote, we simply get a NaN value.

# So how does this work?
# Under the hood the first parquet file that we wrote has not changed. It still has the original schema, with the `alias` column.

# +------+-----------------+--------------+-------------------------------------------+--------------+----------+
# |   id | name            | alias        | powers                                    |   birth_year | active   |
# |------+-----------------+--------------+-------------------------------------------+--------------+----------|
# |    1 | Scott Summers   | Cyclops      | Optic blasts, team leadership             |         1970 | True     |
# |    2 | Jean Grey       | Phoenix      | Telepathy, telekinesis, Phoenix Force     |         1972 | True     |
# ...

# In contrast the new parquet file has the re-named column `codename` instead and the new columnn `first_appearance` right before the `active` column:

# $ parquet-tools show ./data/00000-0-d4edb0b3-a2b5-48b7-9166-de05a7d783a4.parquet
# +------+------------------+------------+--------------------------------------------+--------------+-------------------------+----------+
# |   id | name             | codename   | powers                                     |   birth_year | first_appearance        | active   |
# |------+------------------+------------+--------------------------------------------+--------------+-------------------------+----------|
# |   15 | Illyana Rasputin | Magik      | Teleportation through Limbo and dark magic |         1982 | Giant-Size X-Men #1     | True     |
# |   18 | Roberto da Costa | Sunspot    | Solar energy absorption and super strength |         1984 | Marvel Graphic Novel #4 | True     |
# +------+------------------+------------+--------------------------------------------+--------------+-------------------------+----------+


# Without iceberg these parquet files would be incompatible.
# If the reader you choose to view them is positioned based then the columns no longer line up and the data types are even different.
# For a name based reader one could match the existing values for the `active` column and just add NaN for the new `first_appearance` one.
# But because of the re-name `alias` -> `codename` the reader could not make sense of the change.

# Iceberg fixes this by not being based on position or name, but instead referencing fields by id.
# If we take a look at the new metadata file that has been produced, we will see that `schemas` now has two entries:

# "schemas": [
#   {
#     "type": "struct",
#     "fields": [
#       {
#         "id": 1,
#         "name": "id",
#         "type": "long",
#         "required": false
#       },
#       {
#         "id": 2,
#         "name": "name",
#         "type": "string",
#         "required": false
#       },
#       {
#         "id": 3,
#         "name": "alias",
#         "type": "string",
#         "required": false
#       },
#       {
#         "id": 4,
#         "name": "powers",
#         "type": "string",
#         "required": false
#       },
#       {
#         "id": 5,
#         "name": "birth_year",
#         "type": "long",
#         "required": false
#       },
#       {
#         "id": 6,
#         "name": "active",
#         "type": "boolean",
#         "required": false
#       }
#     ],
#     "schema-id": 0,
#     "identifier-field-ids": []
#   },
#   {
#     "type": "struct",
#     "fields": [
#       {
#         "id": 1,
#         "name": "id",
#         "type": "long",
#         "required": false
#       },
#       {
#         "id": 2,
#         "name": "name",
#         "type": "string",
#         "required": false
#       },
#       {
#         "id": 3,
#         "name": "codename",
#         "type": "string",
#         "required": false
#       },
#       {
#         "id": 4,
#         "name": "powers",
#         "type": "string",
#         "required": false
#       },
#       {
#         "id": 5,
#         "name": "birth_year",
#         "type": "long",
#         "required": false
#       },
#       {
#         "id": 7,
#         "name": "first_appearance",
#         "type": "string",
#         "required": false
#       },
#       {
#         "id": 6,
#         "name": "active",
#         "type": "boolean",
#         "required": false
#       }
#     ],
#     "schema-id": 1,
#     "identifier-field-ids": []
#   }
# ],

# Here every field has an id, and we can see that the field with id `3` had the name `alias` in the first version and in the latest it is called `codename`.
# Now if we inspect the [file metadata](https://parquet.apache.org/docs/file-format/metadata/) of our parquet files using

# $ parquet-tools meta ./data/00000-0-d4edb0b3-a2b5-48b7-9166-de05a7d783a4.parquet | jq

# We find inside each `SchemaElement` a `field_id`.
# For example in the most recently written parquet file we can find the following:
# {
#   "PathInSchema": [
#     "codename"
#   ],
#   "Type": "BYTE_ARRAY",
#   "RepetitionType": "OPTIONAL",
#   "ConvertedType": "convertedtype=UTF8",
#   "LogicalType": "logicaltype=STRING",
#   "FieldID": 3,
#   "Encodings": [
#     "PLAIN",
#     "RLE",
#     "RLE_DICTIONARY"
#   ],
#   "CompressedSize": 98,
#   "UncompressedSize": 80,
#   "NumValues": 2,
#   "NullCount": 0,
#   "MaxValue": "Sunspot",
#   "MinValue": "Magik",
#   "FileOffset": 0,
#   "DataPageOffset": 303,
#   "DictionaryPageOffset": 260,
#   "EncodingStats": [
#     {
#       "PageType": "DICTIONARY_PAGE",
#       "Encoding": "PLAIN",
#       "Count": 1
#     },
#     {
#       "PageType": "DATA_PAGE",
#       "Encoding": "RLE_DICTIONARY",
#       "Count": 1
#     }
#   ],
#   "SizeStatistics": {
#     "UnencodedByteArrayDataBytes": 12,
#     "DefinitionLevelHistogram": [
#       0,
#       2
#     ]
#   },
#   "CompressionCodec": "ZSTD"
# },

# When appending the data, the Iceberg writer took care of embedding these file ids into the data file.
# This way when a reader now scans the table it can now project the data of the underlying data files onto the most recent schema, even through they may have been written with an older one.

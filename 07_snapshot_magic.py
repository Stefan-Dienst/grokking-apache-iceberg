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
from iceberg.setup import (
    create_marvel_xmen_namespace,
    load_sqlite_catalog,
    setup_warehouse,
)

setup_warehouse()

catalog = load_sqlite_catalog()
create_marvel_xmen_namespace(catalog)


df = read_csv("./x-men.csv")

table = catalog.create_table(
    identifier="xmen.characters",
    schema=df.schema,
)


# Intro goes in blog

# Let's first add 10 x-men to our database.
table.append(df)
print(len(table.scan().to_arrow()))
# > 10

# Then we can create a tag to reference this snapshot.
# A tag is just a name, which can be used to reference a snapshot instead of using it's id.
table.manage_snapshots().create_tag(
    snapshot_id=table.current_snapshot().snapshot_id, tag_name="v1"
).commit()

# In the metadata file this shows up like this:
# "v1": {
#       "snapshot-id": s1,
#       "type": "tag"
#     }


# Now we can add three more x-men to create a another snapshot:
df = read_csv("./x-men2.csv")

table.append(df)
print(len(table.scan().to_arrow()))
# > 13

# Notice now that in the metadata there is always the `main` branch.
# This is the current state of the table and we can now see that it points to the latest snapshot, while the tag `v1` still points to the old one:

# "refs": {
#     "main": {
#       "snapshot-id": s2,
#       "type": "branch"
#     },
#     "v1": {
#       "snapshot-id": s1,
#       "type": "tag"
#     },
# }

# But the thing is the old snapshot s1 still exists.
# By adding new data nothing was overwritten, no information of how we eneded up in this state was lost.
# Everything is still there, captured in the hierarchy of metadata files.
# Therefore we can just query an older state of the table if we want to:
v1_snapshot_id = table.refs()["v1"].snapshot_id
print(len(table.scan(snapshot_id=v1_snapshot_id).to_arrow()))
# > 10

# This feature of looking at a previous state of a table goes by the name of time travel.
# Typically this feature is show cased by running a query for a table as it looked like at a specific point in time, e.g.
# SELECT count(*) FROM xmen.characters TIMESTAMP AS OF '2026-08-16 22:45:00'
# But note, that this does not magically offer to travel time freely.
# Behind the scenes just the snapshot with a timestamp closest before the given timestamp is selected, see

# def snapshot_as_of_timestamp(self, timestamp_ms: int, inclusive: bool = True) -> Snapshot | None:
#     """Get the snapshot that was current as of or right before the given timestamp, or None if there is no matching snapshot.

# Hence, if your data has some kind of creation time semantics that differ from how you commit new data to the Iceberg table, you may get suprising results.


# Anyways, let's now craete a new branch.
# In contrast to tags, branches are not bound to a single snapshot, but move.
# When new data is commited to them, and with it a new snapshot created, they automatically update to point to this new snapshot.
# In the end this is just like git branches behaves.
# (And thinking about it, Iceberg snapshots shares a lot of similarities with git's commits. See here for a [nice git deep dive](https://jwiegley.github.io/git-from-the-bottom-up/).)

# We can create a new branch via:
table.manage_snapshots().create_branch(
    snapshot_id=table.current_snapshot().snapshot_id, branch_name="dev"
).commit()

# And then only append two more x-men to this branch
df = read_csv("./x-men4.csv")
table.append(df, branch="dev")
print(len(table.scan().to_arrow()))
# > 13

# What happened now is that a new snapshot was craeted but only the dev branch references it.

# "refs": {
#   "main": {
#     "snapshot-id": s2,
#     "type": "branch"
#   },
#   "v1": {
#     "snapshot-id": s1,
#     "type": "tag"
#   },
#   "dev": {
#     "snapshot-id": s3,
#     "type": "branch"
#   }
# },

# This branch feature is perfect for patterns like the write-audit-publish (WAP) pattern.
# In this pattern new data is first written, then it is checked if it matches the data quality requirements (audit), and the depending on the output of the audit it is either published, i.e. declared the new state of the table or dismissed.
# In this WAP sprite, Iceberg allows us to query the latest newley written, but yet unpublished data, via
dev_snapshot_id = table.refs()["dev"].snapshot_id
audit_scan = table.scan(snapshot_id=dev_snapshot_id).to_arrow()

# on which we could run some data quality checks.
# And if they all passed we could publish them, i.e. set the current snapshot of the table to it using.
table.manage_snapshots().set_current_snapshot(ref_name="dev").commit()

# Then when simply querying the default state of the table, we now see the full 15 x-men:
print(len(table.scan().to_arrow()))
# > 15

# Grokking Apache Iceberg

This repo contains code for examples using Apache Iceberg and accompanies the blogs post: [Grokking Apache Iceberg](https://thingsworthsharing.dev/iceberg/).
Please read the blog first and only look here if you want to dig deeper.

## Requirements
 - [uv](https://docs.astral.sh/uv/)
 - docker for the hive metastore section (HMS), see `./hms/`
 - rust for the equality delete part in `./rust/`

## Usage
Change the `./iceberg/config.py` to fit your needs.

Then just run for the example you want to see the following, e.g. `uv run 01_catalog.py`.
Then you can inspect the filesystem structure in your `WAREHOUSE_PATH`.

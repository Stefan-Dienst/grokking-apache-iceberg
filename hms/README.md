# Hive Meta Store Example

This is a small example to investigate what the Hive Meta Store (HMS) stores when creating a table.
It works by launching the following components via docker:
 - A HMS instance
 - A postgreSQL database as the storage backend for the HMS
 - Minio as an object storage

For details see the `docker-compose.yml` file.

Then PySpark is used to create a database and a table and insert data into the latter.

## Requirements
 - docker
 - You need to have the postgreSQL driver available. Download it with: `wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar`

## Steps 
 1. `docker compose up -d`
 2. `uv run spark.py`
 3. Inspect the underlying data of the HMS by connecting to `localhost:5432` with your tool of choice.


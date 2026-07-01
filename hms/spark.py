from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("hms-demo")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .config("spark.sql.hive.metastore.version", "4.1.0")
    .config("spark.sql.hive.metastore.jars", "maven")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sql("SHOW DATABASES").show()

spark.sql("CREATE DATABASE IF NOT EXISTS xmen_db")
spark.sql("USE xmen_db")

spark.sql("""
CREATE TABLE IF NOT EXISTS xmen (
    name STRING,
    codename STRING,
    power STRING
)
PARTITIONED BY (joined_year INT)
STORED AS PARQUET
LOCATION 's3a://warehouse/xmen'
""")

spark.sql("""
INSERT INTO xmen PARTITION (joined_year=1963) VALUES
    ('Scott Summers', 'Cyclops', 'Optic blasts'),
    ('Jean Grey', 'Phoenix', 'Telepathy/Telekinesis')
""")

spark.sql("""
INSERT INTO xmen PARTITION (joined_year=1975) VALUES
    ('Ororo Munroe', 'Storm', 'Weather control'),
    ('Kurt Wagner', 'Nightcrawler', 'Teleportation')
""")

spark.sql("SELECT * FROM xmen").show()

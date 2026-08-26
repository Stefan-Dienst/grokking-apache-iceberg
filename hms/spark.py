from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("hms-demo")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .config("spark.sql.hive.metastore.version", "4.1.0")
    .config("spark.sql.hive.metastore.jars", "maven")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    # S3A configuration
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.driver.memory", "512m")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.cores", "2")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sql("SHOW DATABASES").show()

spark.sql("CREATE DATABASE IF NOT EXISTS xmen_db")
spark.sql("USE xmen_db")

spark.sql("""
CREATE TABLE IF NOT EXISTS xmen (
    id INT,
    name STRING,
    alias STRING,
    powers STRING,
    birth_year INT
)
PARTITIONED BY (active BOOLEAN)
STORED AS PARQUET
LOCATION 's3a://warehouse/xmen'
""")

spark.sql("""
INSERT INTO xmen PARTITION (active=true) VALUES
    (1, 'Scott Summers', 'Cyclops', 'Optic blasts, team leadership', 1970),
    (2, 'Jean Grey', 'Phoenix', 'Telepathy, telekinesis, Phoenix Force', 1972),
    (3, 'Logan', 'Wolverine', 'Regeneration, adamantium claws', 1880),
    (4, 'Ororo Munroe', 'Storm', 'Weather control, flight', 1975),
    (6, 'Kurt Wagner', 'Nightcrawler', 'Teleportation, wall crawling', 1978),
    (9, 'Kitty Pryde', 'Shadowcat', 'Phasing through objects', 1985),
    (10, 'Anna Marie', 'Rogue', 'Power absorption', 1979)
    DISTRIBUTE BY 1  -- Force single file
""")

spark.sql("""
INSERT INTO xmen PARTITION (active=false) VALUES
    (5, 'Hank McCoy', 'Beast', 'Super strength, agility, genius intellect', 1968),
    (7, 'Pietro Maximoff', 'Quicksilver', 'Super speed', 1980),
    (8, 'Charles Xavier', 'Professor X', 'Telepathy, mind control', 1940)
    DISTRIBUTE BY 1
""")

spark.sql("SELECT * FROM xmen").show()

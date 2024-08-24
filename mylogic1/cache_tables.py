# cache_tables.py;

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CacheTables") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

# Cache radius table
print("Caching radius table...")
spark.sql("CACHE TABLE radius_cache SELECT * FROM delta.`/DATA/deltalake-radius`")

# Cache syslog table
print("Caching syslog table...")
spark.sql("CACHE TABLE syslog_cache SELECT * FROM delta.`/DATA/deltalake-syslog`")

print("Tables cached successfully!")
spark.stop()
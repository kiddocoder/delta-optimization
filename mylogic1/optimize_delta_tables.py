# optimize_delta_tables.py;

from pyspark.sql import SparkSession;
from delta.tables import DeltaTable;

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OptimizeDeltaTables") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

# Optimize radius table
print("Optimizing radius table...")
radius_table = DeltaTable.forPath(spark, "/DATA/deltalake-radius")
radius_table.optimize().zOrder("IMSI", "StartTime").executeCompaction()

# Optimize syslog table
print("Optimizing syslog table...")
syslog_table = DeltaTable.forPath(spark, "/DATA/deltalake-syslog")
syslog_table.optimize().zOrder("Source IP", "StartTime").executeCompaction()

print("Optimization complete!")
spark.stop()
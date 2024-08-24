# optimize_small_deltalakes.py

from pyspark.sql import SparkSession;
from delta.tables import DeltaTable;
import time

spark = SparkSession.builder \
    .appName("MonitorSmallDeltaLakes") \
    .config("spark.sql.files.maxPartitionBytes", "134217728"  # 128MB
    ).config("spark.sql.shuffle.partitions", "8"  # Reduced for smaller datasets
    ).config("spark.default.parallelism", "8"  # Adjusted for smaller datasets
    ).config("spark.sql.adaptive.enabled", "true"
    ).config("spark.sql.adaptive.coalescePartitions.enabled", "true"
    ).config("spark.sql.adaptive.skewJoin.enabled", "true"
    ).config("spark.memory.offHeap.enabled", "true"
    ).config("spark.memory.offHeap.size", "1g"
    ).config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0" \
    ).getOrCreate();

def optimize_table(path, zorder_cols):
    start_time = time.time()
    table = DeltaTable.forPath(spark, path)
    table.optimize().zOrder(zorder_cols).executeCompaction()
    end_time = time.time()
    return end_time - start_time

print("Optimizing radius table...")
radius_time = optimize_table("/DATA/deltalake-radius", ["Source IP", "StartTime", "IMSI"])
print(f"Radius table optimization time: {radius_time:.2f} seconds")

print("Optimizing syslog table...")
syslog_time = optimize_table("/DATA/deltalake-syslog", ["Source IP", "StartTime"])
print(f"Syslog table optimization time: {syslog_time:.2f} seconds")

print(f"Total optimization time: {radius_time + syslog_time:.2f} seconds")

spark.stop()
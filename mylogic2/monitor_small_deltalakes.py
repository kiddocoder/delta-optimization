# monitor_small_deltalakes.py;

from pyspark.sql import SparkSession;
import time;

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

# Define the query
query = """
SELECT r.*, s.*
FROM delta.`/DATA/deltalake-radius` r
JOIN delta.`/DATA/deltalake-syslog` s ON r.`Source IP` = s.`Source IP`
WHERE r.IMSI = '65'
  AND r.StartTime >= '1709031275'
  AND r.StopTime <= '1709031280'
  AND s.StartTime >= '1709031275'
  AND s.StopTime <= '1709031280'
"""

# Execute the query and measure time
start_time = time.time()
result = spark.sql(query)
end_time = time.time()
query_time = end_time - start_time

# Collect execution metrics
query_execution = result._jdf.queryExecution()
physical_plan = query_execution.executedPlan()

print("Query Metrics:")
print(physical_plan.metrics())

# Print some basic statistics
print("\nBasic Statistics:")
partition_count = result.rdd.getNumPartitions()
print(f"Number of partitions: {partition_count}")

count_start_time = time.time()
row_count = result.count()
count_end_time = time.time()
count_time = count_end_time - count_start_time

print(f"Number of rows: {row_count}")
print(f"Query execution time: {query_time:.2f} seconds")
print(f"Count operation time: {count_time:.2f} seconds")
print(f"Total operation time: {query_time + count_time:.2f} seconds")

# Show the query plan
print("\nQuery Plan:")
result.explain()

spark.stop()
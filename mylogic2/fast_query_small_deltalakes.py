# fast_query_small_deltalakes.py;

from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
            .appName("FastQuerySmallDeltaLakes") \
            .config("spark.sql.files.maxPartitionBytes", "134217728"  # 128MB
            ).config("spark.sql.shuffle.partitions", "8"  # Reduced for smaller datasets
            ).config("spark.default.parallelism", "8"  # Adjusted for smaller datasets
            ).config("spark.sql.adaptive.enabled", "true"
            ).config("spark.sql.adaptive.coalescePartitions.enabled", "true"
            ).config("spark.sql.adaptive.skewJoin.enabled", "true"
            ).config("spark.memory.offHeap.enabled", "true"
            ).config("spark.memory.offHeap.size", "1g"
            ).config("spark.sql.inMemoryColumnarStorage.compressed", "true"
            ).config("spark.sql.inMemoryColumnarStorage.batchSize", "10000"
            ).config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0" \
            ).getOrCreate()

# Cache tables
print("Caching tables...")
cache_start = time.time()
spark.sql("CACHE TABLE radius SELECT * FROM delta.`/DATA/deltalake-radius`")
spark.sql("CACHE TABLE syslog SELECT * FROM delta.`/DATA/deltalake-syslog`")
cache_end = time.time()
print(f"Caching time: {cache_end - cache_start:.2f} seconds")

# Define the query
query = """
SELECT r.*, s.*
FROM radius r
JOIN syslog s ON r.`Source IP` = s.`Source IP`
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

# Show a sample of the results
print("Sample results:")
result.show(5, truncate=False)

# Print execution time
query_time = end_time - start_time
print(f"Query execution time: {query_time:.2f} seconds")

# Count rows and measure time
count_start_time = time.time()
total_rows = result.count()
count_end_time = time.time()
count_time = count_end_time - count_start_time

print(f"Total rows: {total_rows}")
print(f"Count operation time: {count_time:.2f} seconds")

print(f"Total operation time: {query_time + count_time:.2f} seconds")

# Uncache tables
spark.sql("UNCACHE TABLE radius")
spark.sql("UNCACHE TABLE syslog")

spark.stop()
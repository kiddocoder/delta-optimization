# optimized_query.py;

from pyspark.sql import SparkSession
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OptimizedQuery") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

# Define the query
query = """
WITH filtered_radius AS (
  SELECT *
  FROM delta.`/DATA/deltalake-radius`
  WHERE IFNULL(IMSI, '0') = '65'
    AND IFNULL(StartTime,'0') >= '1709031275'
    AND IFNULL(StopTime,'0') <= '1709031280'
)
SELECT r.*, s.*
FROM filtered_radius r
JOIN delta.`/DATA/deltalake-syslog` s
  ON r.`Source IP` = s.`Source IP`
WHERE s.StartTime >= '1709031275'
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
print(f"Query execution time: {end_time - start_time:.2f} seconds")

# Print the count of rows
print(f"Total rows: {result.count()}")

spark.stop()
# monitor_query.py;

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MonitorQuery") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

# Define the query
query = """
SELECT r.*, s.*
FROM delta.`/DATA/deltalake-radius` r
JOIN delta.`/DATA/deltalake-syslog` s
  ON r.`Source IP` = s.`Source IP`
WHERE r.IMSI = '65'
  AND r.StartTime >= '1709031275'
  AND r.StopTime <= '1709031280'
  AND s.StartTime >= '1709031275'
  AND s.StopTime <= '1709031280'
"""

# Execute the query
result = spark.sql(query)

# Collect execution metrics
query_execution = result._jdf.queryExecution()
physical_plan = query_execution.executedPlan()

print("Query Metrics:")
print(physical_plan.metrics())

# Print some basic statistics
print("\nBasic Statistics:")
print(f"Number of partitions: {result.rdd.getNumPartitions()}")
print(f"Number of rows: {result.count()}")

# Show the query plan
print("\nQuery Plan:")
result.explain()

spark.stop()
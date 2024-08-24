## To run these optimized scripts:

Execute each script using:
Copyspark-submit --packages io.delta:delta-core_2.12:1.0.0 script_name.py

## Run the scripts in this order:

1. optimize_small_deltalakes.py
2. fast_query_small_deltalakes.py
3. monitor_small_deltalakes.py

## These optimizations include:

1. Adjusted partition sizes and parallelism for smaller datasets.
2. Enabled adaptive query execution.
3. Enabled off-heap memory usage.
4. Compressed in-memory columnar storage.
5. Optimized batch size for in-memory storage.
6. Caching tables in memory for faster access.
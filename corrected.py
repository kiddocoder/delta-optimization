from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Charger les tables Delta
delta_table1 = DeltaTable.forPath(spark, "/DATA/deltalake-radius")
delta_table2 = DeltaTable.forPath(spark, "/DATA/deltalake-syslog")

# Appliquer les optimisations (si nécessaire)
delta_table1.optimize().executeCompaction()
delta_table2.optimize().executeCompaction() 

# Appliquer les filtres aux tables et faire la jointure
filtered_df1 = delta_table1.toDF().filter("(Source IP='-275551957') AND (startTime>='1709031275')")
filtered_df2 = delta_table2.toDF().filter("(Source IP='-275551957') AND (startTime>='1709031275')")

# Faire la jointure des deux DataFrames sur la colonne 'Source IP'
joined_df = filtered_df1.join(filtered_df2, filtered_df1["Source IP"] == filtered_df2["Source IP"])

# Afficher le résultat
joined_df.show()
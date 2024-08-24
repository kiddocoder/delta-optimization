import duckdb
from deltalake import DeltaTable, write_deltalake
import pandas as pd

# and here is how i create my delta lake
# (deplicating file pour augmenter
# le size de delta lake :

for i in range(40):
    print('starting',i+1)
    # def ip_to_int(ip):
    #     """Convertit une adresse IP en entier."""
    #     if isinstance(ip, bytes):  # Convertir bytearray en string
    #         ip = ip.decode('utf-8')
    #     octets = ip.split('.')
    #     return (int(octets[0]) << 24) + (int(octets[1]) << 16) + (int(octets[2]) << 8) + int(octets[3])

    def ip_to_uint32(ip):
    # """Convertit une adresse IP en entier non signé de 32 bits."""
        if isinstance(ip, bytes):  # Convertir bytearray en string
            ip = ip.decode('utf-8')
        octets = ip.split('.')
        int_ip = (int(octets[0]) << 24) + (int(octets[1]) << 16) + (int(octets[2]) << 8) + int(octets[3])
        return int_ip & 0xFFFFFFFF  # Assurer que l'entier est sur 32 bits non signés






    query = f"""SELECT * FROM read_parquet('/DATA/syslogpipline2parquet-data/*.parquet') ;"""
    df1 = duckdb.sql(query).df()



    # Convertir les données bytearray en chaînes de caractères pour toutes les colonnes concernées
    for column in ["Source IP", "Destination IP", "NAT Source IP"]:
        df1[column] = df1[column].apply(lambda x: x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x)

    # Appliquer la conversion des adresses IP en entier
     # 3. Optimize IP Data Type (STRING -> INTEGER):
    df1["Source IP"] = df1["Source IP"].apply(ip_to_uint32).astype("int32")
    df1["Destination IP"] = df1["Destination IP"].apply(ip_to_uint32).astype("int32")
    df1["NAT Source IP"] = df1["NAT Source IP"].apply(ip_to_uint32).astype("int32")

    # Exemple de requêtes SQL avec DuckDB pour obtenir des DataFrames mis à jour
    result_df1 = duckdb.sql("""
        SELECT
            "StartTime",
            "StopTime" ,
            "Source IP",
            "IMSI",
            "MSISDN",
            "IMEI",
            "User Location Info"

        FROM df1

    """).df()

    result_df2 = duckdb.sql("""
        SELECT
            "StartTime",
            "StopTime",
            "Duration",
            "Source IP",
            "Destination IP",
            "Destination Port",
            "NAT Source IP",
            "NAT Source Port",
            "ID Protocol",
            "Bytes from Client",
            "Bytes from Server",
            "Session ID"
        FROM df1
    """).df()

    # Écriture dans Delta Lake avec partitionnement
    write_deltalake(f"/DATA/deltalake-radius", result_df1, partition_by=["Source IP","StartTime"], mode='append')
    write_deltalake(f"/DATA/deltalake-syslog", result_df2, partition_by=["Source IP","StartTime"], mode='append')

    # write_deltalake(f"./delta-lake-radius", result_df1, partition_by=["Source IP"], mode='append')
    # write_deltalake(f"./delta-lake-syslog", result_df2, partition_by=["Source IP"], mode='append')

# from delta.tables import DeltaTable

# Assuming you have a SparkSession initialized:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("DeltaOptimization").getOrCreate()

# radius_table = DeltaTable.forPath(spark, f"./delta-lake-radius")
# radius_table.optimize().zOrder("IMSI")

# syslog_table = DeltaTable.forPath(spark, f"./delta-lake-syslog")
# syslog_table.optimize().zOrder("Destination IP") # Choose the most appropriate column(s)

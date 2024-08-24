from deltalake import DeltaTable
import duckdb
import pandas as pd
import pyarrow
import logging
import time

#

# ///if filterType in two table and columns in two table :imsI=98 columns : all
# 1. Exécuter la requête sur la première table pour récupérer les données
start_time1 = time.time()
print("starting ")
columnsTableOne = duckdb.query("""
    SELECT table1.*
    FROM delta_scan('/DATA/deltalake-radius') AS table1
    WHERE IFNULL(table1."IMSI", '0') = '65'
    AND IFNULL(table1."StartTime",'0') >= '1709031275'
    AND IFNULL(table1."StopTime",'0') <= '1709031280'

""").fetchall()
# print(columnsTableOne)
end_time1 = time.time()

duration1 = end_time1 - start_time1
print("duration requete table1:",duration1)
# print(columnsTableOne)
# # 2. Extraire les valeurs de la colonne "Source IP"
# Extraire les valeurs de la colonne "Source IP" (3ème colonne, indice 2)
print("starting extrait les ips")
start_time2 = time.time()
listIp = [row[2] for row in columnsTableOne]
end_time2 = time.time()

durationIp=end_time2-start_time2
print("duration IPS:",durationIp)
# Obtenir uniquement les adresses IP uniques
unique_ips = list(set(listIp))

# print(unique_ips)

# # 3. Construire la requête pour la deuxième table en utilisant les IP filtrées
# #    Assurez-vous que 'Source IP' correspond à l'index correct de la colonne dans votre table.
print("starting query 2")
start_time3=time.time()
queryTableTwo = f"""
    SELECT table2.*
    FROM delta_scan('/DATA/deltalake-syslog') AS table2
    WHERE table2."Source IP" IN ({', '.join(f"'{ip}'" for ip in listIp)})
    AND IFNULL(table2."StartTime",'0') >= '1709031275'
    AND IFNULL(table2."StopTime",'0') <= '1709031280'
"""

# # 4. Exécuter la requête sur la deuxième table
columnsTableTwo = duckdb.query(queryTableTwo).fetchall()
end_time3=time.time()
duration3=end_time3-start_time3
print("duration requete table2:",duration3)

print("starting the combination :")

start_time4=time.time()
# Concaténer les résultats des deux requêtes
combined_results = columnsTableOne + columnsTableTwo
end_time4=time.time()
duration4=end_time4-start_time4
# Afficher les résultats combinés
# print("Combined Results:", combined_results)
print("duration combination",duration4)
print("duration of all: " ,end_time4-start_time1)
# # 5. Afficher ou traiter les résultats
# print(columnsTableTwo)

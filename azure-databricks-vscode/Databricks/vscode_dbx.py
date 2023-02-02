from databricks import sql
import os

with sql.connect(server_hostname = "adb-422859057363798.18.azuredatabricks.net",
                 http_path       = "/sql/1.0/endpoints/e4d19a191fb37b53",
                 access_token    = "dapi7f9558ba937c099d36c9d50200ccc77c-2") as connection:

  with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM default.diamonds LIMIT 2")
    result = cursor.fetchall()

    for row in result:
      print(row)

# Connexion JDBC Databricks
#jdbc:databricks://adb-422859057363798.18.azuredatabricks.net;HttpPath=/sql/1.0/warehouses/e4d19a191fb37b53;TransportMode=http;SSL=1;EnableArrow=0
#Token DbSchema : dapi667d4be3cd6e3dcb26a1f4a362946e88-2
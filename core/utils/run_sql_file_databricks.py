from databricks import sql
from datetime import datetime
import json


# Databricks connection params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

databricks_host = conn_params.get("databricks_prod_server_hostname")
databricks_cluster_path = conn_params.get("databricks_prod_cluster_path")
databricks_token = conn_params.get("databricks_prod_token")



def execute_sql_file(query_):
    # Cluster
    connection = sql.connect(server_hostname = databricks_host,
                             http_path = databricks_cluster_path,
                             access_token = databricks_token)
    query_list = query_.split(";")
    while("\n" in query_list):
        query_list.remove("\n")
    while("" in query_list):
        query_list.remove("")
    query_list = [item.strip() for item in query_list]
    print("Databriks run sql file start... ", datetime.now())
    try:
        cursor = connection.cursor()
        for q in query_list:
            print("------- ------- ------- \n",q)
            cursor.execute(q)
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Databriks run sql file finish... ", datetime.now())
    finally:
        cursor.close()
        connection.close()

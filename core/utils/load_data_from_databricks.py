import pandas as pd
from databricks import sql
from datetime import datetime
import json



# Databricks connection params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

databricks_host = conn_params.get("databricks_prod_server_hostname")
databricks_cluster_path = conn_params.get("databricks_prod_cluster_path")
databricks_token = conn_params.get("databricks_prod_token")


def get_data_from_sql_query(query_):
    # Cluster
    connection = sql.connect(server_hostname = databricks_host,
                             http_path = databricks_cluster_path,
                             access_token = databricks_token)
    print("Databriks run sql script start... ", datetime.now())
    df_ = pd.DataFrame()
    try:
        cursor = connection.cursor()
        cursor.execute(query_)

        column_names = [desc[0] for desc in cursor.description]
        df_ = pd.DataFrame.from_records(cursor.fetchall(), columns=column_names)
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Databriks run sql script end... ", datetime.now())
    finally:
        cursor.close()
        connection.close()
    return df_

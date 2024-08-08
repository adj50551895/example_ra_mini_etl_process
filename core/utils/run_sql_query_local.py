import mysql.connector
from datetime import datetime
import json
import pandas as pd

# Load database connetion params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")
edx_schema_ = conn_params.get("edx_schema")


def get_data_from_sql_query_local_database(query_):
    dbConnection = mysql.connector.connect(user=local_user_, 
                                           password=local_pass_,
                                           host=local_host_,
                                           port=local_port_,
                                           database=edx_schema_)
    try:
        print("Get data from sql query start... ", datetime.now())
        with dbConnection.cursor() as cursor:
            cursor.execute(query_)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_ = pd.DataFrame(rows, columns = column_names, index = None)
            del rows
        print("Get data from sql query end... ", datetime.now())
    finally:
        dbConnection.close()
    return df_

def delete_data_from_sql_query_local_database(query_):
    dbConnection = mysql.connector.connect(user=local_user_, 
                                           password=local_pass_,
                                           host=local_host_,
                                           port=local_port_,
                                           database=edx_schema_)
    try:
        print("Delete data from sql query start... ", datetime.now())
        with dbConnection.cursor() as cursor:
            cursor.execute(query_)
            dbConnection.commit()
        print("Delete data from sql query end... ", datetime.now())
    finally:
        dbConnection.close()

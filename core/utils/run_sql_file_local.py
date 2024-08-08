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


def execute_sql_file_on_local_database(file_):
    print("SQL file: "+file_+" run START...", datetime.now())
    sql_file = open(file_, "r", encoding="utf-8")
    data = sql_file.read()

    dbConnection = mysql.connector.connect(user=local_user_,
                                           password=local_pass_,
                                           host=local_host_,
                                           port=local_port_,
                                           database=edx_schema_)
    try:
        conn = dbConnection.cursor() # conn = dbConnection.cursor(buffered=True)
        results = conn.execute(data, multi=True)
        for cur in results:
            cur
            #print(" ---- sql cursor ---- \n", cur)
            #if cur.with_rows:
                #print('result:', cur.fetchall())
        dbConnection.commit() # ako ima commit u fajlovima ne treba ovde
    finally:
        print("SQL file: "+file_+" run END...", datetime.now())
        dbConnection.close()


def get_data_from_sqlfile_local_database(sqlfilePath_):
    dbConnection = mysql.connector.connect(user=local_user_, 
                                           password=local_pass_,
                                           host=local_host_,
                                           port=local_port_,
                                           database=edx_schema_)
    query_ = open(sqlfilePath_, "r", encoding="utf-8").read()
    try:
        with dbConnection.cursor() as cursor:
            cursor.execute(query_)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_ = pd.DataFrame(rows, columns = column_names, index = None)
            del rows
    finally:
        dbConnection.close()
    return df_
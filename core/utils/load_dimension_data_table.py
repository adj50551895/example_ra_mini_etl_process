import pymysql
import pandas as pd
from datetime import datetime
import json


# Load local database connetion params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")


def get_table_data(table):
    dbConnection = pymysql.connect(
        host=local_host_, user=local_user_,
        password=local_pass_, port=local_port_
    )
    print(table+" data load started...", datetime.now())
    try:
        with dbConnection.cursor() as cursor:
            query = "select t.* from "+table+" t;"
            cursor.execute(query)
            rows = cursor.fetchall()

            column_names = [i[0] for i in cursor.description]
            df_ = pd.DataFrame(rows, columns = column_names, index = None)
            del rows
    finally:
        print(table+" data load finished...", datetime.now())
        dbConnection.close()
    return df_
import trino
import pandas as pd
#from sqlalchemy import create_engine
from datetime import datetime
import json
from turbodbc import connect, make_options

# Hadoop Presto database connection params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

hadoop_host_ = conn_params.get("hadoop_host")
hadoop_port_ = conn_params.get("hadoop_port")
hadoop_http_scheme_ = conn_params.get("hadoop_http_scheme")
hadoop_catalog_ = conn_params.get("hadoop_catalog")
hadoop_schema_ = conn_params.get("hadoop_schema")
hadoop_username_ = conn_params.get("hadoop_username")
hadoop_password_ = conn_params.get("hadoop_password")

#Trino
def get_hadoop_data(query_):
    dbConnectionPresto = trino.dbapi.connect(
        host=hadoop_host_,
        port=hadoop_port_,
        http_scheme=hadoop_http_scheme_,
        catalog=hadoop_catalog_,
        schema=hadoop_schema_,
        auth=trino.auth.BasicAuthentication(hadoop_username_, hadoop_password_),
    )
    print("Presto data load started...", datetime.now())
    df_ = pd.DataFrame()
    try:
        cursor = dbConnectionPresto.cursor()
        cursor.execute(query_)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        df_ = pd.DataFrame(rows, columns = column_names, index = None)
        del rows
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Presto data load finish...", datetime.now())
    finally:
        dbConnectionPresto.close()
    return df_

#Hive
def get_hive_data(query_):
    try:
        print("Hive data load started...", datetime.now())
        options = make_options(autocommit=True)
        connection = connect(dsn='Adobe_Hive', turbodbc_options=options) # ODBC connection
        df_ = pd.read_sql_query(query_, connection) 
    except ValueError as vx:
        print(vx)
    except Exception as ex:   
        print(ex)
    else:
        print("Hive data load finish...", datetime.now())
    finally:
        connection.close()
    return df_
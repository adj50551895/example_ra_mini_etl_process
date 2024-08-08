import trino
from trino.transaction import IsolationLevel
from datetime import datetime
import json


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


def execute_sql_file_in_hadoop(query_):
    with trino.dbapi.connect(
        host=hadoop_host_,
        port=hadoop_port_,
        http_scheme=hadoop_http_scheme_,
        catalog=hadoop_catalog_,
        schema=hadoop_schema_,
        auth=trino.auth.BasicAuthentication(hadoop_username_, hadoop_password_),
        isolation_level=IsolationLevel.AUTOCOMMIT
    ) as conn:
        print("Presto run sql file started...", datetime.now())
        query_list = query_.split(";")
        while("\n" in query_list):
            query_list.remove("\n")
        while("" in query_list):
            query_list.remove("")
        query_list = [item.strip() for item in query_list]
        try:
            cursor = conn.cursor()
            for q in query_list:
                print("------------->\n",q)
                cursor.execute(q)
                cursor.fetchall()
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
        else:
            print("Presto run sql file finish... ", datetime.now())
        finally:
            conn.close()

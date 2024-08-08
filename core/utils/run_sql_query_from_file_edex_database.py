# ## only one query can be saved in a sql file

import sys
sys.path.append("import")
from datetime import datetime
import json
import pymysql
from sshtunnel import SSHTunnelForwarder
import paramiko


# Load database connection parameters (AWS EdEx Connection)
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

mypkey = paramiko.RSAKey.from_private_key_file(conn_params.get("edex_mypkey_path"))


rds_hostname = conn_params.get("edex_rds_hostname")
sql_hostname = conn_params.get("edex_sql_hostname")
sql_username = conn_params.get("edex_sql_username")
sql_password = conn_params.get("edex_sql_password")
sql_main_database = conn_params.get("edex_sql_main_database")
sql_port = conn_params.get("edex_sql_port")
ssh_host = conn_params.get("edex_ssh_host")
ssh_user = conn_params.get("edex_ssh_user")
ssh_port = conn_params.get("edex_ssh_port")



def run_sql_query_from_file_in_edex_database(file_):
    print("SQL file: "+file_+" run start...", datetime.now())
    sql_file = open(file_, "r", encoding="utf-8")
    
    data = sql_file.read()

    tunnel = SSHTunnelForwarder((ssh_host, ssh_port),
                                 ssh_username = ssh_user,
                                 ssh_pkey=mypkey,
                                 remote_bind_address = (rds_hostname, sql_port)
        );
    tunnel.start() # start the tunnel
        
    dbConnection = pymysql.connect(
        host=sql_hostname, user=sql_username,
        password=sql_password, port=tunnel.local_bind_port
    )
    try:
        print(data)
        conn = dbConnection.cursor() # conn = dbConnection.cursor(buffered=True)
        # results = conn.execute(data, multi=True)
        results = conn.execute(data)
        print(results)
        dbConnection.commit() # ako ima commit u fajlovima ne treba ovde
        sql_file.close()
    finally:
        print("SQL file: "+file_+" run end...", datetime.now())
        dbConnection.close()

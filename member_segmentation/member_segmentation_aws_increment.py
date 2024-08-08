import sys
sys.path.append("import")
import pymysql
import pandas as pd
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
import paramiko
import core.utils.load_dimension_data_table as load_dimension_data_table
import core.utils.data_utils as data_utils
import json


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



# import local table
df_memberSegmentationLocal = load_dimension_data_table.get_table_data("edx.Membersegmentation")


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
    with dbConnection.cursor() as cursor:
        table = "ex_application.MemberSegmentation"
        print(table+" AWS data load started...", datetime.now())

        # create backup table on AWS
        backupTable = "ex_application.MemberSegmentation_backup"
        data_utils.insert_to_backup_table(dbConnection, table, backupTable, dropBackupTable_ = 1)
        dbConnection.commit()

        # read source table data - ex_application.MemberSegmentation
        query = "SELECT t.* FROM "+table+" t;"
        cursor.execute(query)            
        rows = cursor.fetchall()

        column_names = [i[0] for i in cursor.description]
        df_memberSegmentationAWS = pd.DataFrame(rows, columns = column_names, index = None)
        del rows

        # find differences (memberId, class) between ex_application.MemberSegmentation and edx.Membersegmentation
        columnKey = ["memberId", "class", "rule"] # columnKey = ["memberId", "class"] 21.12.2022
        df_merge_insert = data_utils.df_process_stage_data_insert(df_memberSegmentationLocal, df_memberSegmentationAWS, columnKey) #columnKey
          

        # delete memebers that are in the existing aws table and not in the new segmentation
        columnKey_delete = ["memberId"]
        df_merge_delete = data_utils.df_process_stage_data_insert(df_memberSegmentationAWS, df_memberSegmentationLocal, columnKey_delete)

        # select necessary columns needed for MemberSegmentation
        df_insert = df_memberSegmentationLocal[df_memberSegmentationLocal.memberId.isin(df_merge_insert.memberId)]

        if not df_merge_delete.empty:
            table = "ex_application.MemberSegmentation"
            tableHist = "ex_application.MemberSegmentation_hist"
            key = ["memberId"]
            # insert changes to hist table
            data_utils.insert_df_data_to_hist_table_by_ids(df_merge_delete, dbConnection, table, tableHist, key)

            # delete changed data from original table
            data_utils.delete_df_data_from_table_by_ids(df_merge_delete, dbConnection, table, key) # delete by memberdId
            dbConnection.commit()

        if not df_insert.empty:
            table = "ex_application.MemberSegmentation"
            tableHist = "ex_application.MemberSegmentation_hist"
            key = ["memberId"]
            # insert changes to hist table
            data_utils.insert_df_data_to_hist_table_by_ids(df_insert, dbConnection, table, tableHist, key)

            # delete changed data from original table
            data_utils.delete_df_data_from_table_by_ids(df_insert, dbConnection, table, key) # delete by memberdId
            dbConnection.commit()

            print("Insert to "+table+" started...", datetime.now())
            df_insert = df_insert.drop_duplicates() 
            data_utils.insert_df_data_to_table(df_insert, dbConnection, table)
            dbConnection.commit()
            print("Insert to "+table+" finished...", datetime.now())
finally:
    dbConnection.close()
tunnel.stop()

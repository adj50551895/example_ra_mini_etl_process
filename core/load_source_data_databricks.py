from databricks import sql
import pandas as pd
from datetime import datetime
import json


# Databricks params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)


databricks_host_ = conn_params.get("databricks_prod_server_hostname")
databricks_cluster_path_ = conn_params.get("databricks_prod_cluster_path")
databricks_token_ = conn_params.get("databricks_prod_token")



# Load source sql query files
open_sql_file_json = open("import\\params\\sql_source_table.json", "r")
sql_file_json = json.load(open_sql_file_json)


# Presto connection
def get_source_data_from_databricks_prod():
    listDataDataFrame_ = []
    connection = sql.connect(server_hostname = databricks_host_,
                             http_path = databricks_cluster_path_,
                             access_token = databricks_token_)
    cursor = connection.cursor()
    
    print("(Databricks) Source data load started...", datetime.now())
    try:
        # [0] edu_memberSegmentation
        query_file_path = sql_file_json.get("databricks_enterprise_dim_org_education")
        query = open(query_file_path, "r", encoding="utf-8").read()

        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        df_databricks_ = pd.DataFrame(rows, columns = column_names, index = None)
        listDataDataFrame_.append(df_databricks_)
        del df_databricks_
        del rows

        # [1] enterpriseMemberLicenseDelegation
        query_file_path = sql_file_json.get("databricks_enterprise_member_license_delegation")
        query = open(query_file_path, "r", encoding="utf-8").read()

        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        df_databricks_ = pd.DataFrame(rows, columns = column_names, index = None)
        listDataDataFrame_.append(df_databricks_)
        del df_databricks_
        del rows

        # [2] enterpriseEdexMemberSegmentationExtract
        query_file_path = sql_file_json.get("databricks_edex_member_segmentation_extract")
        query = open(query_file_path, "r", encoding="utf-8").read()

        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        df_databricks_ = pd.DataFrame(rows, columns = column_names, index = None)
        listDataDataFrame_.append(df_databricks_)
        del df_databricks_
        del rows

    finally:
        cursor.close()
        connection.close()
    print("(Databricks) Source data load finish...", datetime.now())
    return listDataDataFrame_


def get_source_enterprise_dim_org_education_raw():    
    return listSourceData[0]

def get_source_ent_member_licence_delegation_raw():    
    return listSourceData[1]

def get_source_edex_member_segmentation_extract_raw():    
    return listSourceData[2]

listSourceData = get_source_data_from_databricks_prod()




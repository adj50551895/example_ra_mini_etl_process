import trino
import pandas as pd
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


# Load source sql query files
open_sql_file_json = open("import\\params\\sql_source_table.json", "r")
sql_file_json = json.load(open_sql_file_json)


# Presto connection
def get_source_data_from_presto():
    listPrestoDataDataFrame_ = []
    dbConnectionPresto = trino.dbapi.connect(
        host=hadoop_host_,
        port=hadoop_port_,
        http_scheme=hadoop_http_scheme_,
        catalog=hadoop_catalog_,
        schema=hadoop_schema_,
        auth=trino.auth.BasicAuthentication(hadoop_username_, hadoop_password_),
    )
    print("(Presto) Source data load started...", datetime.now())
    try:
        # [0] edu_memberSegmentation, user_gk.edu_memberSegmentation
        query_file_path = sql_file_json.get("hadoop_enterprise_dim_org_education")
        query = open(query_file_path, "r", encoding="utf-8").read()

        cursor = dbConnectionPresto.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        df_edu_memberSegmentationRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
        listPrestoDataDataFrame_.append(df_edu_memberSegmentationRaw_)
        del df_edu_memberSegmentationRaw_
        del rows

        # [1] enterpriseMemberLicenseDelegation
        query_file_path = sql_file_json.get("hadoop_source_enterprise_member_license_delegation")
        query = open(query_file_path, "r", encoding="utf-8").read()

        cursor = dbConnectionPresto.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        df_segmentationGuidsExtRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
        listPrestoDataDataFrame_.append(df_segmentationGuidsExtRaw_)
        del df_segmentationGuidsExtRaw_
        del rows 

        # [2] enterpriseEdexMemberSegmentationExtract
        query_file_path = sql_file_json.get("hadoop_source_edex_member_segmentation_extract")
        query = open(query_file_path, "r", encoding="utf-8").read()

        cursor = dbConnectionPresto.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        df_EdexMemberSegmentationExtractRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
        listPrestoDataDataFrame_.append(df_EdexMemberSegmentationExtractRaw_)
        del df_EdexMemberSegmentationExtractRaw_
        del rows 
    finally:
        dbConnectionPresto.close()
    print("(Presto) Source data load finish...", datetime.now())
    return listPrestoDataDataFrame_


def get_source_enterprise_dim_org_education_raw():    
    return listSourceData[0]

def get_source_ent_member_licence_delegation_raw():    
    return listSourceData[1]

def get_source_edex_member_segmentation_extract_raw():    
    return listSourceData[2]

listSourceData = get_source_data_from_presto()
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

def get_hdp_dim_data():   
    listDimDataDataFrame_ = [] # empty list of Pandas df
    dbConnection = pymysql.connect(
        host=local_host_, user=local_user_,
        password=local_pass_, port=local_port_
    )
    print("(Presto) Dimension data load started: ", datetime.now())
    try:
        with dbConnection.cursor() as cursor:
            # [0] - eduMemberSegmentationRaw, user_gk.edu_memberSegmentation, presto
            query = "select t.* from hdp.enterprise_dim_org_education t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimEnterpriseDimOrgEducation_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimEnterpriseDimOrgEducation_)
            del df_dimEnterpriseDimOrgEducation_
            del rows

            # [1] - EntMemberLicenseDelegationHadoopDim
            query = "select t.* from hdp.EntMemberLicenseDelegation t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataEntMemberLicDelegation_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataEntMemberLicDelegation_)
            del df_dimDataEntMemberLicDelegation_
            del rows

            # [2] - EdexMemberSegmentationExtract
            query = "select t.* from hdp.EdexMemberSegmentationExtract t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataEdexMemberSegmentationExtract_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataEdexMemberSegmentationExtract_)
            del df_dimDataEdexMemberSegmentationExtract_
            del rows
    finally:
        print("(Presto) Dimension data load finished: ", datetime.now())
        dbConnection.close()
    #tunnel.stop() # stop the tunnel
    return listDimDataDataFrame_

#get_dim_edu_member_segmentation
def get_dim_enterprise_dim_org_education():
    return listDimData[0]

def get_dim_data_ent_member_licence_delegation():
    return listDimData[1]

def get_dim_data_edex_member_segmentation_extract():
    return listDimData[2]

listDimData = get_hdp_dim_data()
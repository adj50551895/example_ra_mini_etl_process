import pymysql
import pandas as pd
from datetime import datetime
import json

import core.load_source_data_hadoop as load_source_data_hadoop
import core.load_dimension_data_hadoop as load_dimension_data_hadoop
import core.utils.data_utils as data_utils


# Load local database connetion params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")

# Presto
# Source
#df_sourceEduMemberSegmentation = load_source_data_hadoop.get_source_edu_member_segmentation_raw()
df_sourceEnterpriseDimOrgEducation = load_source_data_hadoop.get_source_enterprise_dim_org_education_raw()
df_sourceEntMemberLicenseDelegation = load_source_data_hadoop.get_source_ent_member_licence_delegation_raw()
df_sourceEdexMemberSegmentationExtract = load_source_data_hadoop.get_source_edex_member_segmentation_extract_raw()


# EdEx
# Dimensions
#df_dimEduMemberSegmentation = load_dimension_data_hadoop.get_dim_edu_member_segmentation()
df_dimEnterpriseDimOrgEducation = load_dimension_data_hadoop.get_dim_enterprise_dim_org_education()
df_dimEntMemberLicenseDelegation = load_dimension_data_hadoop.get_dim_data_ent_member_licence_delegation()
df_dimEdexMemberSegmentationExtract = load_dimension_data_hadoop.get_dim_data_edex_member_segmentation_extract()



# 3 Create codebooks
# ## UPDATE, INSERT
dbConnection = pymysql.connect(
    host=local_host_, user=local_user_,
    password=local_pass_, port=local_port_
)
try:
    print("(Presto) Dimension process started: ", datetime.now())
    
    # [0] EduMemberSegmentation
    #table = "hdp.EduMemberSegmentation"
    table = "hdp.enterprise_dim_org_education"
    #columnKey = ["org_id", "market_subsegment", "class_"]
    columnKey = list(df_sourceEnterpriseDimOrgEducation.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceEnterpriseDimOrgEducation, df_dimEnterpriseDimOrgEducation, columnKey)

    if not df_merge_insert.empty:
        df_merge_insert = df_merge_insert.drop_duplicates()
        primaryKey = ["org_id"]
        #data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, primaryKey)
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert = df_merge_insert.drop_duplicates() 
        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "hdp.enterprise_dim_org_education_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print("* "+table+" processed...", datetime.now())

    # [1] EnterpriseMemberLicenseDelegation
    table = "hdp.EntMemberLicenseDelegation"
    #columnKey = ["member_guid", "org_id", "delegation_status"]
    columnKey = list(df_sourceEntMemberLicenseDelegation.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceEntMemberLicenseDelegation, df_dimEntMemberLicenseDelegation, columnKey)

    if not df_merge_insert.empty:
        df_merge_insert = df_merge_insert.drop_duplicates()
        primaryKey = ["member_guid"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)
    
        df_merge_insert = df_merge_insert.drop_duplicates()
        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)
        
        tableHist = "hdp.EntMemberLicenseDelegation_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print("* "+table+" processed...", datetime.now())

    # [2] EdexMemberSegmentationExtract
    table = "hdp.EdexMemberSegmentationExtract"
    #columnKey = ["memberguid", "adobeguid", "member_created", "business_group", "target_group", "email", "country", "state_province"]
    columnKey = list(df_sourceEdexMemberSegmentationExtract.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceEdexMemberSegmentationExtract, df_dimEdexMemberSegmentationExtract, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["memberguid"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert = df_merge_insert.drop_duplicates()
        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)
        
        tableHist = "hdp.EdexMemberSegmentationExtract_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print("* "+table+" processed...", datetime.now())

finally:
    print("(Presto) Dimension process finished: ", datetime.now())
    dbConnection.close()
    
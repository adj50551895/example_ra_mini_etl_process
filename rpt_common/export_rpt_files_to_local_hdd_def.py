import sys
sys.path.append("import") # because load_dimension_data_table is in another directory
import datetime
import json
import core.utils.run_sql_file_local as run_sql_file_local
#import pandas as pd
import csv


conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")


current_date = datetime.datetime.now().strftime("%Y%m%d")


# Load sql query files
open_sql_file_json = open("import\\params\\sql_file_scripts.json", "r")
sql_file_json = json.load(open_sql_file_json)


# Content Detail
def export_rpt_content_detail():
    # Courses
    query_file_path = sql_file_json.get("rpt_content_detail_courses_final")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_content_courses = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-content-detail\\content_detail_courses_"+current_date+".csv"
    df_content_courses.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print("1.) rpt_content_detail_courses_final has been exported")

    # Teaching resources
    query_file_path = sql_file_json.get("rpt_content_detail_tr_final")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_content_tr = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-content-detail\\content_detail_tr_"+current_date+".csv"
    df_content_tr.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    del df_content_courses
    del df_content_tr
    print("2.) rpt_content_detail_tr_final has been exported")


# Dashboard
def export_rpt_dashboard():
    query_file_path = sql_file_json.get("rpt_dashboard_fy_final_table")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_dashboard = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-dashboard\\dashboard_"+current_date+".csv"
    df_dashboard.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print("3.) rpt_dashboard_fy_final_table has been exported")

    # Dashboard dimensions
    query_file_path = sql_file_json.get("rpt_dashboard_dim_combination")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_dashboard_dim = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-dashboard\\dashboard_dim_combination_"+current_date+".csv"
    df_dashboard_dim.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    del df_dashboard
    del df_dashboard_dim
    print("4.) rpt_dashboard_dim_combination has been exported")


# District Report
def export_rpt_district():
    # Activity by District K12
    query_file_path = sql_file_json.get("rpt_district_report_activity_by_district_final")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_district_activity_k12 = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-district-report\\k12\\district_report_activity_by_district_"+current_date+".csv"
    df_district_activity_k12.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print("5.) rpt_district_report_activity_by_district_final has been exported")

    # Activity by Monthly K12
    query_file_path = sql_file_json.get("rpt_district_report_activity_monthly_final")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_district_monthly_k12 = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-district-report\\k12\\district_report_activity_monthly_"+current_date+".csv"
    df_district_monthly_k12.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print("6.) rpt_district_report_activity_monthly_final has been exported")

    # Activity by District HED
    query_file_path = sql_file_json.get("rpt_district_report_activity_by_district_hed_final")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_district_activity_hed = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-district-report\\hed\\district_report_activity_by_district_hed_"+current_date+".csv"
    df_district_activity_hed.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print("7.) rpt_district_report_activity_by_district_hed_final has been exported")

    # Activity by Monthly HED
    query_file_path = sql_file_json.get("rpt_district_report_activity_monthly_hed_final")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_district_monthly_hed = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-district-report\\hed\\district_report_activity_monthly_hed_"+current_date+".csv"
    df_district_monthly_hed.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    del df_district_activity_k12
    del df_district_monthly_k12
    del df_district_activity_hed
    del df_district_monthly_hed
    print("8.) rpt_district_report_activity_monthly_hed_final has been exported")


# Fullstory Visitors Dashboard
def export_rpt_fullstory():
    query_file_path = sql_file_json.get("rpt_fullstroy_visitors_dashboard_final")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_fullstrory = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-fullstory\\fullstroy_visitors_dashboard_"+current_date+".csv"
    df_fullstrory.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    del df_fullstrory
    print("9.) rpt_fullstroy_visitors_dashboard_final has been exported")


# User Sign-Ups
def export_rpt_user_sign_ups():
    query_file_path = sql_file_json.get("rpt_user_sign_ups")
    query = open(query_file_path, "r", encoding="utf-8").read()

    df_user_sign_ups = run_sql_file_local.get_data_from_sqlfile_local_database(query_file_path)
    output_csv_path = "import\\export\\rpt-user-sign-ins\\user_sign_ins_"+current_date+".csv"
    df_user_sign_ups.to_csv(output_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    del df_user_sign_ups
    print("10.) rpt_user_sign_ups has been exported")


# dataframes_in_memory = [var for var in globals() if isinstance(globals()[var], pd.DataFrame)]
# print(dataframes_in_memory)

import sys
sys.path.append("import") # because load_dimension_data_table is in another directory
import json
import core.utils.run_sql_file_local as run_sql_file_local



conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")


# Load sql query files
open_sql_file_json = open("import\\params\\sql_file_scripts.json", "r")
sql_file_json = json.load(open_sql_file_json)


# content_detail_courses
def run_content_detail_courses():
    query_file_path = sql_file_json.get("rpt_content_detail_courses")
    run_sql_file_local.execute_sql_file_on_local_database(query_file_path)

# content_detail_courses
def run_dashboard_fy():
    query_file_path = sql_file_json.get("rpt_dashboard_fy")
    run_sql_file_local.execute_sql_file_on_local_database(query_file_path)

# content_detail_courses
def run_district_report():
    query_file_path = sql_file_json.get("rpt_district_report")
    run_sql_file_local.execute_sql_file_on_local_database(query_file_path)
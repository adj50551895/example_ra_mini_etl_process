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


# stage_events_acquisition_content
def load_stage_events_acquisition_content():
    query_file_path = sql_file_json.get("stage_events_acquisition_content")
    #query = open(query_file_path, "r", encoding="utf-8").read()
    run_sql_file_local.execute_sql_file_on_local_database(query_file_path)

# stage_member_first_time_content_acquisition
def load_stage_member_first_time_content_acquisition():
    query_file_path = sql_file_json.get("stage_member_first_time_content_acquisition")
    #query = open(query_file_path, "r", encoding="utf-8").read()
    run_sql_file_local.execute_sql_file_on_local_database(query_file_path)

# stage_edex_express_dates
def load_stage_edex_express_dates():
    query_file_path = sql_file_json.get("stage_edex_express_dates")
    #query = open(query_file_path, "r", encoding="utf-8").read()
    run_sql_file_local.execute_sql_file_on_local_database(query_file_path)

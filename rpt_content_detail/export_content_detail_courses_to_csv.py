import sys
sys.path.append("import")
import json
import core.utils.run_sql_query_local as run_sql_query_local
#import core.utils.insert_data_to_local_database as insert_data_to_local_database
#import pandas as pd
import core.utils.data_utils as data_utils
from datetime import date
#from datetime import datetime


conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")


# Load sql query files
open_sql_file_json = open("import\\params\\sql_file_scripts.json", "r")
sql_file_json = json.load(open_sql_file_json)

query_file_path = sql_file_json.get("rpt_content_detail_courses_final")
query = open(query_file_path, "r").read()

df_content_detail_courses = run_sql_query_local.get_data_from_sql_query_local_database(query)


# Export to file
path = "import\export\\rpt-content-detail\\"
fileName = "content_detail_courses_AAA_" + date.today().strftime('%Y%m%d')
fileExtension = ".csv"
fileSeparator = ","

# export to file
data_utils.export_data_to_file(df_content_detail_courses, path, fileName, fileExtension, fileSeparator, archive_="", fileHeader_=True)



import json
import sys
sys.path.append("import") # because load_dimension_data_table_v1 is in another directory
import core.utils.run_sql_file_edex_database as run_sql_file_edex_database


sql_file = open("import\\params\\sql_file_scripts.json", "r")
sql_params = json.load(sql_file)

#sql_file_path = "C:\document\edex_project\member_segmentation\sql\process_daily_edexmembersegmentation_table.sql"
sql_file_path = sql_params.get("process_daily_edexmembersegmentation_table")


run_sql_file_edex_database.run_sql_query_from_file_in_edex_database(sql_file_path)


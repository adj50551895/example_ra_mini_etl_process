import sys
sys.path.append("import")
import json
from datetime import datetime
import core.utils.insert_data_to_local_database as insert_data_to_local_database
import fullstory_data.fullstory_core as fullstory_core

# Load connetion params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)
fullstory_token = conn_params.get("fullstory_token")
fullstory_visitors_us_file_path = conn_params.get("fullstory_visitors_us_file_path")
fullstory_visitors_row_file_path = conn_params.get("fullstory_visitors_row_file_path")


date_from = "2023-04-29"
date_to = "2023-06-02"

now = datetime.now()

# US visitors - from fullstory export to file
segmentId = "sZM0wDWUC2cG"
fullstory_file_name = "visitors_us_"+datetime.now().strftime("%Y%m%d_%H%M%S")+".csv.gz"
fullstory_core.export_fullstory_segment_date(fullstory_token, segmentId, date_from, date_to, date_from, date_to, fullstory_file_name, fullstory_visitors_us_file_path)

# US visitors - read data from downloaded file
df_us = fullstory_core.import_fullstory_data_from_file(conn_params.get("fullstory_visitors_us_file_path"))
df_us["segmentId"] = segmentId
df_us["segmentName"] = "visitors_us"
df_us["date_imported"] = now.strftime("%m/%d/%Y %H:%M:%S")
df_us = df_us.drop_duplicates()
if not df_us.empty:
    df_us["UserVars"] = df_us["UserVars"].apply(fullstory_core.convert_math_bold_to_utf8)

# US visitors - insert to local database fullstory table
if not df_us.empty:
    schema = "ra"
    table = "fullstory_visitors"
    #insert_data_to_local_database.insert_data_to_table(schema, table, df_us, if_exists_="replace")
    insert_data_to_local_database.insert_new_df_rows_to_table(schema, table, df_us, ["IndvId", "Uid", "LastPage", "segmentId"])
    

# US visitors - move file to imported folder
fullstory_core.moveFullstoryFilesToimported(conn_params.get("fullstory_visitors_us_file_path"))


# ROW visitors - from fullstory export to file
segmentId = "iQGjLDBvC7Z0"
fullstory_file_name = "visitors_row_"+datetime.now().strftime("%Y%m%d_%H%M%S")+".csv.gz"
fullstory_core.export_fullstory_segment_date(fullstory_token, segmentId, date_from, date_to, date_from, date_to, fullstory_file_name, fullstory_visitors_row_file_path)

# US visitors - read data from file
df_row = fullstory_core.import_fullstory_data_from_file(conn_params.get("fullstory_visitors_row_file_path"))
df_row["segmentId"] = segmentId
df_row["segmentName"] = "visitors_row"
df_us["date_imported"] = now.strftime("%m/%d/%Y %H:%M:%S")
df_row = df_row.drop_duplicates()
if not df_row.empty:
    df_row['UserVars'] = df_row['UserVars'].apply(fullstory_core.convert_math_bold_to_utf8)

# ROW visitors - insert to local database fullstory table
if not df_row.empty:
    schema = "ra"
    table = "fullstory_visitors"
    insert_data_to_local_database.insert_new_df_rows_to_table(schema, table, df_row, ["IndvId", "Uid", "LastPage", "segmentId"])
    #insert_data_to_local_database.insert_new_df_rows_to_table_by_id(schema, table, df_row, "IndvId")

# ROW visitors - move file to imported folder
fullstory_core.moveFullstoryFilesToimported(conn_params.get("fullstory_visitors_row_file_path"))

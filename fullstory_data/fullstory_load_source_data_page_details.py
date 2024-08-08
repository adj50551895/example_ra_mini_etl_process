import sys
sys.path.append("import") 
import json
import core.utils.insert_data_to_local_database as insert_data_to_local_database
import fullstory_data.fullstory_core as fullstory_core
import core.utils.run_sql_file_local as run_sql_file_local
import pandas as pd
from dateutil import parser
from datetime import datetime


# Load connetion params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)
fullstory_token = conn_params.get("fullstory_token")
fullstory_visitors_us_page_details_path = conn_params.get("fullstory_visitors_us_page_details_path")
fullstory_visitors_row_page_details_path = conn_params.get("fullstory_visitors_row_page_details_path")


# Get hana dim date fiscal month
sql_file_scripts = open("import\\params\\sql_file_scripts.json", "r")
sql_file_scripts_params = json.load(sql_file_scripts)
sql_hana_dim_date_load_fiscal_month = sql_file_scripts_params.get("hana_dim_date_load_fiscal_month")
df_fiscal_month = run_sql_file_local.get_data_from_sqlfile_local_database(sql_hana_dim_date_load_fiscal_month)

# Add time to df_fiscal_month. Convert time date for first_month and last_month
time_to_add = pd.Timedelta(hours=22, minutes=0, seconds=00)
df_fiscal_month["first_day"] = df_fiscal_month["first_day"]- pd.Timedelta(days=1)
df_fiscal_month["first_day"] = pd.to_datetime(df_fiscal_month["first_day"])
df_fiscal_month["first_day"] = df_fiscal_month["first_day"] + time_to_add

time_to_add = pd.Timedelta(hours=21, minutes=59, seconds=59)
df_fiscal_month["last_day"] = pd.to_datetime(df_fiscal_month["last_day"])
df_fiscal_month["last_day"] = df_fiscal_month["last_day"] + time_to_add

# delete from fullstory_tmp_visitors_page_details_fiscal_month table
run_sql_file_local.execute_sql_file_on_local_database("import\\fullstory_data\\sql\\fullstory_delete_fiscal_month_to_tmp_visitors_page_details.sql")

print("Start Visitors US / Not bots...", datetime.now())
# US visitors - read data from downloaded file
segmentId = "sZM0wDWUC2cG"
df_us = fullstory_core.import_fullstory_data_from_file(conn_params.get("fullstory_visitors_us_page_details_path"))
df_us = df_us[["IndvId", "UserId", "SessionId", "UserDisplayName", "UserCreated", "PageId", "PageUrl", "PageRefererUrl", "ReqUrl", "UserDisplayName", "PageUserAgent", "EventStart", "EventType", "EventSubType", "SessionStart", "PageStart", "PageBrowser", "PageBrowserVersion", "PageDevice", "PagePlatform", "PageOperatingSystem"]]
#df_us = df_us[["IndvId", "UserId", "SessionId", "UserDisplayName", "EventStart", "PageUrl"]]
df_us = df_us.drop_duplicates()
df_us["segmentId"] = segmentId
df_us["segmentName"] = "visitors_page_details_us"

# Convert from 2023-04-28T22:00:03.653Z to date time
df_us["EventStart"] = df_us["EventStart"].apply(parser.isoparse)
df_us["EventStart"] = df_us["EventStart"].dt.strftime("%Y-%m-%d %H:%M:%S")
df_us["EventStart"] = pd.to_datetime(df_us["EventStart"])

# US visitors - insert to local database fullstory table
if not df_us.empty:
    schema = "ra"
    table = "fullstory_tmp_visitors_page_details"
    insert_data_to_local_database.insert_data_to_table(schema, table, df_us, "replace")

# US run sql file on local database
# table fullstory_tmp_visitors_page_details_fiscal_month
run_sql_file_local.execute_sql_file_on_local_database("import\\fullstory_data\\sql\\fullstory_insert_fiscal_month_to_tmp_visitors_page_details.sql")

# US visitors - move file to imported folder
fullstory_core.moveFullstoryFilesToimported(conn_params.get("fullstory_visitors_us_page_details_path"))
print("End Visitors US / Not bots...", datetime.now())


print("Start Visitors ROW / Not bots...", datetime.now())
# ROW visitors - read data from downloaded file
segmentId = "iQGjLDBvC7Z0"
df_row = fullstory_core.import_fullstory_data_from_file(conn_params.get("fullstory_visitors_row_page_details_path"))
df_row = df_row[["IndvId", "UserId", "SessionId", "UserDisplayName", "UserCreated", "PageId", "PageUrl", "PageRefererUrl", "ReqUrl", "UserDisplayName", "PageUserAgent", "EventStart", "EventType", "EventSubType", "SessionStart", "PageStart", "PageBrowser", "PageBrowserVersion", "PageDevice", "PagePlatform", "PageOperatingSystem"]]
df_row = df_row.drop_duplicates()
df_row["segmentId"] = segmentId
df_row["segmentName"] = "visitors_page_details_row"

# Convert from 2023-04-28T22:00:03.653Z to date time
df_row["EventStart"] = df_row["EventStart"].apply(parser.isoparse)
df_row["EventStart"] = df_row["EventStart"].dt.strftime("%Y-%m-%d %H:%M:%S")
df_row["EventStart"] = pd.to_datetime(df_row["EventStart"])

# ROW visitors - insert to local database fullstory table
if not df_row.empty:
    schema = "ra"
    table = "fullstory_tmp_visitors_page_details"
    insert_data_to_local_database.insert_data_to_table(schema, table, df_row, "replace")

# ROW run sql file on local database
# table fullstory_tmp_visitors_page_details_fiscal_month
run_sql_file_local.execute_sql_file_on_local_database("import\\fullstory_data\\sql\\fullstory_insert_fiscal_month_to_tmp_visitors_page_details.sql")

# ROW visitors - move file to imported folder
fullstory_core.moveFullstoryFilesToimported(conn_params.get("fullstory_visitors_row_page_details_path"))
print("End Visitors ROW / Not bots...", datetime.now())


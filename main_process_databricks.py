## Environment: \opt\miniconda3\envs\env_edex_3_9_12\python.exe
import importlib
import os
import sys
sys.path.append("XXXX")
from datetime import datetime
import core.utils.run_sql_file_local as run_sql_file_local
import core.utils.export_local_table_to_file as export_local_table_to_file
import core.utils.run_sql_file_databricks as run_sql_file_databricks
import core.utils.insert_data_to_local_database as insert_data_to_local_database
import core.process_stage_tables_local as process_stage_tables_local

os.chdir("import")

print("Mini ETL process started... ", datetime.now())


print("----------------------- import DIMENSIONS START -----------------------------")

insert_data_to_local_database.insert_to_process_log("START (EdEX) Process dimensions...")
import core.process_dimensions_edex
insert_data_to_local_database.insert_to_process_log("END (EdEX) Process dimensions...")
print("END 1.) ---------------------------------------------------")


# import UD list
insert_data_to_local_database.insert_to_process_log("START (Local) import UD list...")
run_sql_file_local.execute_sql_file_on_local_database("sql\\update_insert_ud_list.sql")
insert_data_to_local_database.insert_to_process_log("END (Local) import UD list...")
print("END 2.) ----------------------------------------------------")


# C:\document\edex_data\export\member-guid-map
insert_data_to_local_database.insert_to_process_log("START (Local) Export member_guid_map to file...")
export_local_table_to_file.export_local_table_to_file("hdp.MemberGuidMap", "/edex_data/export/member-guid-map", "member_guid_map", ".csv", ",", "gzip", True)
insert_data_to_local_database.insert_to_process_log("END (Local) Export member_guid_map to file...")
print("END 3.) ----------------------------------------------------")


# STOP
print("(Databricks) upload member_guid_map.csv file to Databricks manually", datetime.now())
input('Press ENTER to continue...')
print("END 3.1.) ----------------------------------------------------")


# Databricks
insert_data_to_local_database.insert_to_process_log("START (Databricks) Create edex_member_segmentation_extract table...")
sql_file_path = "sql/databricks_create_edex_member_segmentation_extract_table.sql"
query = open(sql_file_path, "r", encoding="utf-8").read()
run_sql_file_databricks.execute_sql_file(query)
insert_data_to_local_database.insert_to_process_log("END (Databricks) Create ccanalytics.edex_member_segmentation_extract table...")
print("END 4.) ----------------------------------------------------")


insert_data_to_local_database.insert_to_process_log("START (Databricks) Process dimensions...")
import core.process_dimensions_databricks
insert_data_to_local_database.insert_to_process_log("END (Databricks) Process dimensions...")
print("END 5.) ----------------------------------------------------")


# MAU
insert_data_to_local_database.insert_to_process_log("START (Databricks) spark_event_activity_b_logins, ccmusg_fact_user_activity_cc_dc - (MAU)...")
import core.load_source_data_databricks_mau_1
insert_data_to_local_database.insert_to_process_log("END (Databricks) spark_event_activity_b_logins, ccmusg_fact_user_activity_cc_dc - (MAU)...")
print("END 6.) ----------------------------------------------------")


insert_data_to_local_database.insert_to_process_log("START (Databricks) Load edex clicks, express clicks - (MAU)...")
import core.load_source_data_databricks_mau_2
insert_data_to_local_database.insert_to_process_log("END (Databricks) Load edex clicks, express clicks - (MAU)...")
print("END 7.) ----------------------------------------------------")
# //MAU
# //Databricks


insert_data_to_local_database.insert_to_process_log("START (Local) Load Elastic Search files...")
import core.load_elasticsearch_files_agg
import core.load_elasticsearch_files_all
insert_data_to_local_database.insert_to_process_log("END (Local) Load Elastic Search files...")
print("END 8.) ----------------------------------------------------")

print("----------------------- import DIMENSIONS FINISHED -----------------------------")


insert_data_to_local_database.insert_to_process_log("START (Local) Process stage tables...")
process_stage_tables_local.load_stage_events_acquisition_content()
process_stage_tables_local.load_stage_member_first_time_content_acquisition()
process_stage_tables_local.load_stage_edex_express_dates()
insert_data_to_local_database.insert_to_process_log("END (Local) Process stage tables...")
print("END 09.) ----------------------------------------------------")
# //Update Stage tables


# Reload dimension data
insert_data_to_local_database.insert_to_process_log("START (Local) Reload dimension data...")
package_name = "core.load_dimension_data_edex"
if package_name in sys.modules:
    print("Reload module ", package_name)
    module = importlib.reload(sys.modules[package_name])

package_name = "core.load_dimension_data_databricks"
if package_name in sys.modules:
    print("Reload module ", package_name)
    module = importlib.reload(sys.modules[package_name])

package_name = "core.load_dimension_data_elasticsearch"
if package_name in sys.modules:
    print("Reload module ", package_name)
    module = importlib.reload(sys.modules[package_name])
insert_data_to_local_database.insert_to_process_log("END (Local) Reload dimension data...")
print("END 10.) ----------------------------------------------------")


# Process Member Segmentation
insert_data_to_local_database.insert_to_process_log("START (Local) Member Segmentation process...")
import member_segmentation.process_member_segmentation
insert_data_to_local_database.insert_to_process_log("END (Local) Member Segmentation process...")
print("END 11.) ----------------------------------------------------")


insert_data_to_local_database.insert_to_process_log("START (local) Run Fullstory workflow ...")
import fullstory_data.fullstory_workflow
insert_data_to_local_database.insert_to_process_log("END (local) Run Fullstory workflow ...")
print("END 12.) ----------------------------------------------------")


# Before you proceed check the reports param values in file:
# import\rpt_params\select_rpt_param_value.sql
insert_data_to_local_database.insert_to_process_log("START (local) Run Content Detail Courses, Dashboard, User Sign Ups and District report ...")
import rpt_common.run_rpt_process_files
insert_data_to_local_database.insert_to_process_log("END (local) Run Content Detail Courses, Dashboard, User Sign Ups and District report ...")
print("END 13.) ----------------------------------------------------")


insert_data_to_local_database.insert_to_process_log("START (local) Export Report results to a csv files ...")
import rpt_common.export_rpt_files_to_local_hdd
insert_data_to_local_database.insert_to_process_log("END (local) Export Reports results to a csv files ...")
print("END 14.) ----------------------------------------------------")


insert_data_to_local_database.insert_to_process_log("START (Edex database) Member Segmentation AWS Increment...")
import member_segmentation.sync_edex_member_segmentation_table
insert_data_to_local_database.insert_to_process_log("END (Edex database) Member Segmentation AWS Increment...")
print("END 15.) ----------------------------------------------------")


insert_data_to_local_database.insert_to_process_log("START (Edex database) Member Segmentation AWS Increment...")
import member_segmentation.member_segmentation_aws_increment
insert_data_to_local_database.insert_to_process_log("END (Edex database) Member Segmentation AWS Increment...")
print("END 16.) ----------------------------------------------------")
# //Process Member Segmentation

print("Mini ETL process finished... ", datetime.now())
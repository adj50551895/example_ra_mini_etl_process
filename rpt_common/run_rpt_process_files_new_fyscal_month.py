import os
import sys
sys.path.append("import")
from datetime import datetime
import core.utils.run_sql_file_local as run_sql_file_local
import core.utils.insert_data_to_local_database as insert_data_to_local_database
import rpt_common.export_rpt_files_to_local_hdd_def as export_rpt_files

os.chdir("import")

print("Mini ETL process for new fiscal month has been started... ", datetime.now())

# Change the FM param values 
# ## rpt_path = "import\\rpt_common\\update_rpt_param_value.sql"
# ## run_sql_file_local.execute_sql_file_on_local_database(rpt_path)


# Download US file from: https://app.fullstory.com/ui/2AP7Y/segments/sZM0wDWUC2cG/people:search?completeSessions=false
# Download ROW file from: https://app.fullstory.com/ui/2AP7Y/segments/iQGjLDBvC7Z0/people/0?completeSessions=false
# Copy US file to: C:\document\edex_data\import\fullstory\visitors_us_page_details\
# Copy ROW file to: C:\document\edex_data\import\fullstory\visitors_row_page_details\
insert_data_to_local_database.insert_to_process_log("START (local) Run Fullstory workflow ...")
import fullstory_data.fullstory_workflow
insert_data_to_local_database.insert_to_process_log("END (local) Run Fullstory workflow ...")
print("END fullstory_data.fullstory_workflow")


# Run Dashboard report
rpt_path = "C:\\document\edex_project\\edex_etl_process\\rpt_dashboard\\dashboard_fy.sql"
run_sql_file_local.execute_sql_file_on_local_database(rpt_path)


# Export Dashboard and Fullstory result
export_rpt_files.export_rpt_dashboard()
export_rpt_files.export_rpt_fullstory()

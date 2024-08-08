import sys
sys.path.append("import")
import core.utils.run_sql_file_local as run_sql_file_local


# 0
# download US file from: https://app.fullstory.com/ui/2AP7Y/segments/sZM0wDWUC2cG/people:search?completeSessions=false
# and copy to: import\fullstory\visitors_us_page_details
#
# download ROW file from: https://app.fullstory.com/ui/2AP7Y/segments/iQGjLDBvC7Z0/people/0?completeSessions=false
# and copy to: import\fullstory\visitors_row_page_details


# 1
import fullstory_data.fullstory_load_source_data_page_details

# 2
import fullstory_data.fullstory_process

# 3
run_sql_file_local.execute_sql_file_on_local_database("import\\fullstory_data\\sql\\fullstory_process_visitor_segmentation.sql")

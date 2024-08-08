import sys
sys.path.append("import")
import core.utils.run_sql_file_local as run_sql_file_local


rpt_path = "import\\rpt_content_detail\\content_detail_courses.sql"
run_sql_file_local.execute_sql_file_on_local_database(rpt_path)

rpt_path = "import\\rpt_content_detail\\content_detail_teaching_resources.sql"
run_sql_file_local.execute_sql_file_on_local_database(rpt_path)

rpt_path = "C:\\document\edex_project\\edex_etl_process\\rpt_dashboard\\dashboard_fy.sql"
run_sql_file_local.execute_sql_file_on_local_database(rpt_path)

rpt_path = "import\\rpt_district_reporting\\district_report.sql"
run_sql_file_local.execute_sql_file_on_local_database(rpt_path)

rpt_path = "import\\rpt_user_sign_ups\\user_sign_ups.sql"
run_sql_file_local.execute_sql_file_on_local_database(rpt_path)

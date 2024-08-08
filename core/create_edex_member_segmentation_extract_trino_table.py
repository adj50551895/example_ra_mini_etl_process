import core.utils.run_sql_file_hadoop_trino as run_sql_file_hadoop_trino


sql_file_path = "import/sql/create_edex_member_segmentation_extract_trino_table.sql"
query = open(sql_file_path, "r", encoding="utf-8").read()


run_sql_file_hadoop_trino.execute_sql_file_in_hadoop(query)
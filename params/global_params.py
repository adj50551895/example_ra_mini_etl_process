import json
import sys
sys.path.append("import\edex_etl_process\params")

# Load local database connetion params
conn_file = open("import\\edex_etl_process\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host = conn_params.get("local_host")
local_user = conn_params.get("local_user")
local_pass = conn_params.get("local_pass")
local_port = conn_params.get("local_port")

databricks_prod_server_hostname = conn_params.get("databricks_prod_server_hostname")
databricks_prod_cluster_path = conn_params.get("databricks_prod_cluster_path")
databricks_prod_cluster_path_1 = conn_params.get("databricks_prod_cluster_path_1")
databricks_prod_cluster_path_2 = conn_params.get("databricks_prod_cluster_path_2")
databricks_prod_sql_warehouse_path = conn_params.get("databricks_prod_sql_warehouse_path")
databricks_prod_token = conn_params.get("databricks_prod_token")

edex_mypkey_path = conn_params.get("edex_mypkey_path")
edex_rds_hostname = conn_params.get("edex_rds_hostname")
edex_sql_hostname = conn_params.get("edex_sql_hostname")
edex_sql_username = conn_params.get("edex_sql_username")
edex_sql_password = conn_params.get("edex_sql_password")
edex_sql_main_database = conn_params.get("edex_sql_main_database")
edex_sql_port = conn_params.get("edex_sql_port")
edex_ssh_host = conn_params.get("edex_ssh_host")
edex_ssh_user = conn_params.get("edex_ssh_user")
edex_ssh_port = conn_params.get("edex_ssh_port")


#import import_params
#print(import_params.edex_sql_hostname)
#a =import_params.edex_sql_hostname

#print(globals())

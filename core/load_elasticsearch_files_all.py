# 19.12.2022 SessionID and event added

### Load Elastic search from files
import sys
sys.path.append("import")
import glob
import os
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json


conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")

path = conn_params.get("es_path")

N_DAYS_AGO = 5000
#dimDateFrom = pd.Timestamp("2023-01-01")
dimDateFrom = ((datetime.now() - timedelta(days=N_DAYS_AGO) - timedelta(days=7))).strftime('%Y-%m-%d %X')
esTableName = "els.events"


def insert_elasticsearch_to_table(df_, dbConnection, table_):
    if not df_.empty:
        columnList = list(df_)
        columnListStr = ", ".join(columnList)
        columnValueslist = []
        with dbConnection.cursor() as cursor:
            for index, row in df_.iterrows():
                for i in range(0, len(df_.columns)):
                    if (isinstance(row[i], str) and len(row[i]) > 100): # Ako je kolona duza od 900 karaktera kastuj na 900, npr. search_type kolona
                        row[i] = row[i][:99]
                    columnValueslist.append(row[i])
                
                query = "INSERT INTO "+table_+" ("+columnListStr+") VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(query,(columnValueslist)) #(row[0],row[1],row[2]))
                columnValueslist = []
                #dbConnection.commit()
        print("ES Rows are inserted.")


def import_elasticsearch_from_file(path_):
    gz_files = glob.glob(os.path.join(path_, "*.gz"))
    gz_files_incr = []
    for fl in gz_files:
        now = datetime.now()
        date_str = fl[len(fl)-17:len(fl)-7]
        date_incr = datetime.strptime(date_str, '%Y-%m-%d')
        if date_incr >= now - timedelta(days=N_DAYS_AGO):
            gz_files_incr.append(fl)

    df_esSourceData_ = pd.DataFrame()
    if len(gz_files_incr) != 0:
        for f in gz_files_incr:
            print(f)
            df_tmp = pd.read_csv(f, compression='gzip', header=0, sep='\t', quotechar='"', low_memory=False, encoding='utf8') #files are UTF-8 encodings
            df_tmp["event_timestamp"] = df_tmp["event_timestamp"].astype("datetime64[ns]")
            df_esSourceData_ = pd.concat([df_esSourceData_, df_tmp])
            del df_tmp
        df_esSourceData_["event_date"] = df_esSourceData_["event_date"].astype("datetime64[ns]")
    return df_esSourceData_


## # start
start = datetime.now()
print(start)

# source, stage data
df_esSourceData = import_elasticsearch_from_file(path)
df_esSourceData["points"] = pd.to_numeric(df_esSourceData["points"])
df_esSourceData["rating"] = pd.to_numeric(df_esSourceData["rating"])
print("ES Stage data read.")

if not df_esSourceData.empty:
    dbConnection = pymysql.connect(
        host=local_host_, user=local_user_,
        password=local_pass_, port=local_port_
    )
    try:
        df_esSourceData = df_esSourceData.fillna(np.nan).replace([np.nan], [None])
        #df_esSourceData = df_esSourceData.fillna(np.nan).replace([np.nan], [None])
        insert_elasticsearch_to_table(df_esSourceData, dbConnection, esTableName)
        dbConnection.commit()
    finally:
        dbConnection.close()
else:
    print("No files to import...")

end = datetime.now()

print(end-start)
print("ES process finished")
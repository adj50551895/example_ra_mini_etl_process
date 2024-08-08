from sqlalchemy import create_engine
from datetime import datetime
import pymysql
import json
import pandas as pd


conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")
ra_schema_ = conn_params.get("ra_schema")


# if_exists - How to behave if the table already exists.
# fail: Raise a ValueError.
# replace: Drop the table before inserting new values.
# append: Insert new values to the existing table.
def insert_data_to_table(schema_, table_, df_, if_exists_="append"):    
    if not df_.empty:
        print("Table %s create/append started."%table_, datetime.now());
        conn_string_ = "mysql+pymysql://"+local_user_+":"+local_pass_+"@"+local_host_+"/"+schema_
        sqlEngine = create_engine(conn_string_, pool_recycle=3600)
        dbLocalConnection_ = sqlEngine.connect()
        try:
            df_.to_sql(table_, dbLocalConnection_, if_exists=if_exists_, index=False);
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print("Table %s created/appended successfully."%table_, datetime.now());
        finally:
            dbLocalConnection_.close()


def insert_to_process_log(description_):
    dbConnection = pymysql.connect(
        host=local_host_, user=local_user_,
        password=local_pass_, port=local_port_
    )
    try:
        with dbConnection.cursor() as cursor:
            query = "INSERT INTO edx.process_log VALUES ('"+description_+"', SYSDATE());"            
            cursor.execute(query)
            dbConnection.commit()
            print(description_)
    finally:
        dbConnection.close()


def insert_new_df_rows_to_table(schema_, table_, df_, ids_):
    conn_string_ = "mysql+pymysql://"+local_user_+":"+local_pass_+"@"+local_host_+"/"+schema_
    sqlEngine = create_engine(conn_string_, pool_recycle=3600)
    dbLocalConnection_ = sqlEngine.connect()
    
    # Fetch existing rows from the table
    existing_data = pd.read_sql(f"SELECT * FROM {table_}", dbLocalConnection_)

    if not df_.empty:
        print("Table %s create/append started."%table_, datetime.now());
        # Identify existing rows based on multiple columns
        existing_rows = existing_data.merge(df_, on=ids_, how='inner')
        
        # Filter new rows not present in the table
        new_rows = df_.merge(existing_rows, on=ids_, how='left', indicator=True)
        new_rows = new_rows.loc[new_rows['_merge'] == 'left_only']
        new_rows = new_rows.drop(columns='_merge')
        new_rows = new_rows[new_rows.columns.drop(list(new_rows.filter(regex='_x')))]
        new_rows = new_rows[new_rows.columns.drop(list(new_rows.filter(regex='_y')))]

        try:
            new_rows.to_sql(table_, dbLocalConnection_, if_exists='append', index=False, chunksize=1000)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print("Table %s created/appended successfully."%table_, datetime.now());
        finally:
            dbLocalConnection_.close()


def insert_new_df_rows_to_table_by_id(schema_, table_, df_, id_):
    conn_string_ = "mysql+pymysql://"+local_user_+":"+local_pass_+"@"+local_host_+"/"+schema_
    sqlEngine = create_engine(conn_string_, pool_recycle=3600)
    dbLocalConnection_ = sqlEngine.connect()
    
    # Fetch existing rows from the table
    existing_data = pd.read_sql(f"SELECT * FROM {table_}", dbLocalConnection_)

    if not df_.empty:
        print("Table %s create/append started."%table_, datetime.now());
        # Filter new rows not present in the table
        new_rows = df_.loc[~df_[id_].isin(existing_data[id_])]
        try:
            new_rows.to_sql(table_, dbLocalConnection_, if_exists='append', index=False, chunksize=1000)
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print("Table %s created/appended successfully."%table_, datetime.now());
        finally:
            dbLocalConnection_.close()
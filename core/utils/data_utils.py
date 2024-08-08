import pandas as pd
import numpy as np
import json


#insert_to_dim_table
def insert_df_data_to_table(df_, dbConnection_, table_):
    cnt_ = 0;
    columnListDf = list(df_)
    columnListStr = ", ".join(columnListDf) #my str su kolone od liste
    columnList = []
    if not df_.empty:
        with dbConnection_.cursor() as cursor:
            for index, row in df_.iterrows():
                for i in range(0, len(df_.columns)):
                    columnList.append(row[i])
                
                query = "INSERT INTO "+table_+" ("+columnListStr+") VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(query,(columnList)) #(row[0],row[1],row[2]))
                cnt_ = cnt_ + 1;
                columnList = []
                #dbConnection.commit()
        print("** "+str(cnt_)+" rows to "+table_+" are inserted.")


# 30.11.2022 Not used
#deleteFromDimTable
def deleteFromDimTable(df_, dbConnection_, table_, key_):
    if not df_.empty:
        #df_["rule"] = df_["rule"].astype(str)
        cnt_ = 0;
        with dbConnection_.cursor() as cursor:
            for index, row in df_.iterrows():
                if key_: #if key list is not empty
                    del_query = "DELETE FROM "+table_+" WHERE "+key_[0]+" = '"+row[0]+"'"
                    if len(key_) > 1:
                        for x in range(1, len(key_)):
                            if row[x] != None:
                                query_tmp = " AND "+ key_[x] + " = '"+str(row[x])+"'" # bilo = promenio na like
                            else:
                                query_tmp = " AND "+ key_[x] + " is null"
                            del_query = del_query + query_tmp
                    del_query = del_query + ";"
                cursor.execute(del_query)
                cnt_ = cnt_ + 1;
                #print(del_query)
                #dbConnection.commit()
        print("** "+str(cnt_)+" rows from "+table_+" table are deleted.")


# delete using in (list of Ids), only one key column - id, only Ids
# delete_ids_from_table
def delete_df_data_from_table_by_ids(df_, dbConnection, table_, key_):
    if not df_.empty:
        id_list = []
        columnIndex = df_.columns.get_loc(key_[0])
        for index, row in df_.iterrows():
            str = "'"+row[columnIndex]+"'"    # str = "'"+row[0]+"'"
            id_list.append(str)
        str_id_list = ", ".join(id_list)
        with dbConnection.cursor() as cursor:
            if key_: #if key list is not empty
                del_query = "DELETE FROM "+table_+" WHERE "+key_[0]+" IN ("+str_id_list+");"
                cursor.execute(del_query)
                #print(upd_query)
                #dbConnection.commit()
        print("** rows from "+table_+" table are deleted.")


#updateDimTableCloseDateTo
#update_table_close_date_to
def update_dim_table_close_date_to(df_, dbConnection, table_, key_):
    if not df_.empty:
        cnt_ = 0;
        with dbConnection.cursor() as cursor:
            for index, row in df_.iterrows():
                if key_: #if key list is not empty
                    upd_query = "UPDATE "+table_+" SET dateTo = sysdate(), isCurrent = 0  WHERE isCurrent = 1 AND "+key_[0]+" = '"+row[0]+"'"
                    if len(key_) > 1:
                        for x in range(1, len(key_)):
                            row_tmp = str(row[x]).replace("'", "\\'" )
                            query_tmp = " AND "+ key_[x] + " = '"+row_tmp+"'" #str row 10.10.2022 dodao
                            upd_query = upd_query + query_tmp
                    upd_query = upd_query + ";"
                cursor.execute(upd_query)
                #print(upd_query)
                cnt_ = cnt_ + 1;
                #dbConnection.commit()
        print("** "+str(cnt_)+" rows from "+table_+" table are updated.")

# Update using in (list of Ids)
#updateIdDimTableCloseDateTo
#update_id_dim_table_close_dateto, update_table_close_dateto_by_ids
def update_dim_table_close_dateto_by_ids(df_, dbConnection, table_, key_):
    if not df_.empty:
        id_list = []
        columnIndex = df_.columns.get_loc(key_[0])
        for index, row in df_.iterrows():
            #str = "'"+row[0]+"'"
            str = "'"+row[columnIndex]+"'"
            id_list.append(str)
        str_id_list = ", ".join(id_list)
        with dbConnection.cursor() as cursor:
            if key_: #if key list is not empty
                upd_query = "UPDATE "+table_+" SET dateTo = sysdate(), isCurrent = 0  WHERE isCurrent = 1 AND "+key_[0]+" IN ("+str_id_list+");"
                cursor.execute(upd_query)
                #print(upd_query)
                #dbConnection.commit()
        print("** rows from "+table_+" table are updated.")


# Parse Academic level from raw column
#createAcademicLevelFromDF
def create_academic_level_from_df(df_, column_, dictValue_, id_):
    df_academicLevel_ = pd.DataFrame()
    for index, row in df_.iterrows():
        dict_tmp = json.loads(row[column_]) #"academicLevels"
        if isinstance(dict_tmp[dictValue_], str):
            dict_tmp  = {dictValue_: [dict_tmp[dictValue_]]} # PROVERI SAMO AKO JE JEDAN U LISTI 13.10.2022
        df_tmp = pd.DataFrame(dict_tmp[dictValue_], columns =[column_])  #"academicLevel"
        df_tmp[id_] = row[id_]
        #df_academicLevel_ = df_academicLevel_.append(df_tmp[[id_, column_]], ignore_index=True) ## 04.11.2022
        df_academicLevel_ = pd.concat([df_academicLevel_, df_tmp[[id_, column_]]], ignore_index=True)
    return df_academicLevel_


# Parse Element from raw column
def create_element_from_df(df_, column_, dictValue_, id_):
    df_element_ = pd.DataFrame()
    for index, row in df_.iterrows():
        dict_tmp = json.loads(row[column_]) #"academicLevels" #"products"
        if isinstance(dict_tmp[dictValue_], str):
            dict_tmp  = {dictValue_: [dict_tmp[dictValue_]]}
        df_tmp = pd.DataFrame(dict_tmp[dictValue_], columns =[column_]) #"academicLevel" #"products"
        df_tmp[id_] = row[id_]
        df_element_ = pd.concat([df_element_, df_tmp[[id_, column_]]], ignore_index=True)
    return df_element_


#createDFFromListOfDict
def create_df_from_list_of_dict(df_, column_, dictValue_, id_):
    df_ListOfDict_ = pd.DataFrame()
    for index, row in df_.iterrows():
        list_tmp = json.loads(row[column_]) # list of dictionaries
        for dict_x in list_tmp:
            if not (dict_x[dictValue_] == None or dict_x[dictValue_] == ""):
                data_id = [df_[id_][index]]
                data_value = [dict_x[dictValue_]]
                #df_ListOfDict_ = df_ListOfDict_.append(list(zip(data_id, data_value)), ignore_index=True) #26.04.2023
                df_ListOfDict_ = pd.concat([df_ListOfDict_, pd.DataFrame(list(zip(data_id, data_value)))], ignore_index=True) # 26.04.2023 added due to new version of pandas package
    df_ListOfDict_ = df_ListOfDict_.rename(columns={0: id_, 1: dictValue_}, errors="raise")
    return df_ListOfDict_


# Stage data processing, Insert, Update - dateFrom, DateTo, isCurrent. Delete and insert row for CreatedAt
#dfProcessStageDataInsert
def df_process_stage_data_insert(df_sourceData_, df_dimData_, columnKey_):
    df_merge_ = pd.merge(df_sourceData_, df_dimData_, how="left", on = columnKey_, indicator=True)
    df_merge_insert_ = df_merge_[df_merge_["_merge"].eq("left_only")]
    df_merge_insert_ = df_merge_insert_[columnKey_]

    df_merge_insert_.replace({pd.NaT: None}, inplace=True)
    df_merge_insert_ = df_merge_insert_.fillna(np.nan).replace([np.nan], [None])

    return df_merge_insert_


# 0 - to drop and create backup table, otherwise insert data to existing backup table 
#insertToBackupTable
def insert_to_backup_table(dbConnection_, table_, backupTable_, dropBackupTable_ = 0):
    with dbConnection_.cursor() as cursor:
        if dropBackupTable_ == 0:
            query = "insert into "+backupTable_+" select * from "+table_+";"
            cursor.execute(query)
            print("Backup table "+backupTable_+" is created")
            #dbConnection_.commit()
        else:
            drop_query = "drop table if exists "+backupTable_+";"
            cursor.execute(drop_query)
            print("Table "+backupTable_+" is dropped")

            create_query = "create table "+backupTable_+" as select * from "+table_+";"
            cursor.execute(create_query)
            print("Table "+backupTable_+" is created")
            #dbConnection_.commit()


# 30.11.2022 not used
def delete_all_data_from_table(dbConnection_, table_):
    with dbConnection_.cursor() as cursor:
        deleteall_query = "delete from "+table_+";"
        cursor.execute(deleteall_query)
        print("Table data "+table_+" are deleted")
        #dbConnection_.commit()


#insertHistDataToHistTable
def insert_hist_data_to_hist_table(dbConnection_, table_, tableHist_):
    with dbConnection_.cursor() as cursor:
        query = "INSERT INTO "+tableHist_+" SELECT * FROM "+table_+" WHERE isCurrent = 0;"
        cursor.execute(query)
        #dbConnection.commit()
    print("** Hist data from  "+table_+" are inserted to "+tableHist_)


#deleteHistDataFromMainTable
def delete_hist_data_from_main_table(dbConnection_, table_):
    with dbConnection_.cursor() as cursor:
        query = "DELETE FROM "+table_+" WHERE isCurrent = 0;"
        cursor.execute(query)
        #dbConnection.commit()
    print("** Hist data from  "+table_+" are deleted")


# only one key_ column, id
#insert_data_to_hist_table_by_ids
def insert_df_data_to_hist_table_by_ids(df_, dbConnection, table_, tableHist_, key_):
    if not df_.empty:
        id_list = []
        columnIndex = df_.columns.get_loc(key_[0])
        for index, row in df_.iterrows():
            str = "'"+row[columnIndex]+"'"
            id_list.append(str)
        str_id_list = ", ".join(id_list)
        with dbConnection.cursor() as cursor:
            if key_: #if key list is not empty
                inst_query = "INSERT INTO "+tableHist_+" SELECT * FROM "+table_+" WHERE "+key_[0]+" IN ("+str_id_list+");"
                cursor.execute(inst_query)
                #print(inst_query)
                #dbConnection.commit()
        print("** rows from "+table_+" table are inserted into "+tableHist_)


#extractDataToCsvFile
def export_data_to_file(df_, path_, fileName_, fileExtension_, fileSeparator_, archive_="", fileHeader_=True):
    if archive_ == "gzip":
        fullPath_ = path_+"\\"+fileName_+fileExtension_+".gz"
        df_.to_csv(fullPath_, index=False, compression="gzip", sep = fileSeparator_, header = fileHeader_)
        print("File "+fileName_+" is exported to "+path_)
    elif archive_ == "zip":
        fullPath_ = path_+"\\"+fileName_+fileExtension_+".gz"
        df_.to_csv(fullPath_, index=False, compression="zip", sep = fileSeparator_, header = fileHeader_)
        print("File "+fileName_+" is exported to "+path_)
    else:
        fullPath_ = path_+"\\"+fileName_+fileExtension_
        df_.to_csv(fullPath_, index=False, sep = fileSeparator_, header = fileHeader_)
        print("File "+fileName_+" is exported to "+path_)
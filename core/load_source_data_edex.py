import pymysql
import paramiko
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from datetime import datetime
import json

# Load database connection parameters
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

mypkey = paramiko.RSAKey.from_private_key_file(conn_params.get("edex_mypkey_path"))


rds_hostname = conn_params.get("edex_rds_hostname")
sql_hostname = conn_params.get("edex_sql_hostname")
sql_username = conn_params.get("edex_sql_username")
sql_password = conn_params.get("edex_sql_password")
sql_main_database = conn_params.get("edex_sql_main_database")
sql_port = conn_params.get("edex_sql_port")
ssh_host = conn_params.get("edex_ssh_host")
ssh_user = conn_params.get("edex_ssh_user")
ssh_port = conn_params.get("edex_ssh_port")


# Load source sql query files
open_sql_file_json = open("import\\params\\sql_source_table.json", "r")
sql_file_json = json.load(open_sql_file_json)


# 1 Create tunnel and get raw resources and dim data
def get_source_data_from_edex_aws():
    tunnel = SSHTunnelForwarder((ssh_host, ssh_port),
                                ssh_username = ssh_user,
                                ssh_pkey=mypkey,
                                remote_bind_address = (rds_hostname, sql_port)
        );
    tunnel.start() # start the tunnel
    #print(tunnel.local_bind_port)
    
    dbConnection = pymysql.connect(
        host=sql_hostname, user=sql_username,
        password=sql_password, port=tunnel.local_bind_port
    )
    listSourceDataDataFrame_ = []
    print("(EdEx) Source data load started... ", datetime.now())
    try:
        with dbConnection.cursor() as cursor:
            # [0] - School Type
            print("** Loading SchoolType source data...")
            query_file_path = sql_file_json.get("edex_source_school_type")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)            
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_schoolTypeCodesRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_schoolTypeCodesRaw_)
            del rows

            # [1] - Academic Level Codes
            print("** Loading AcademicLevelCodes source data...")
            query_file_path = sql_file_json.get("edex_source_academic_level_codes")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)            
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_academiLevelCodesRaw__ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_academiLevelCodesRaw__)
            del rows

            # [2] - Members
            print("** Loading Members source data...")
            query_file_path = sql_file_json.get("edex_source_member")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_academiLevelCodesRaw__ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_academiLevelCodesRaw__)
            del rows

            # [3] - Enrollments, All columns imported
            print("** Loading Enrollments source data...")
            query_file_path = sql_file_json.get("edex_source_enrollment")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_enrollmentRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_enrollmentRaw_)
            del rows

            # [4] - Courses, All columns imported
            print("** Loading Courses source data...")
            query_file_path = sql_file_json.get("edex_source_course")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_courseRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_courseRaw_)
            del rows

            # [5] - Resources, All columns imported
            print("** Loading Resources source data...")
            query_file_path = sql_file_json.get("edex_source_resource")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_resourceRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_resourceRaw_)
            del rows

            # [6] - Static Page
            print("** Loading StaticPage source data...")
            query_file_path = sql_file_json.get("edex_static_page")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_staticPageRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_staticPageRaw_)
            del rows

            # [7] - Rating
            print("** Loading Rating source data...")
            query_file_path = sql_file_json.get("edex_rating")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_ratingRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_ratingRaw_)
            del rows

            # [8] - Favorite
            print("** Loading Favorite source data...")
            query_file_path = sql_file_json.get("edex_favorite")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_favoriteRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_favoriteRaw_)
            del rows

            # [9] - PlayList
            print("** Loading PlayList source data...")
            query_file_path = sql_file_json.get("edex_play_list")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_playListRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_playListRaw_)
            del rows

            # [10] - PlayListItem
            print("** Loading PlayListItem source data...")
            query_file_path = sql_file_json.get("edex_play_list_item")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_playListItemRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_playListItemRaw_)
            del rows

            # [11] - MemberToBadge
            print("** Loading MemberToBadge source data...")
            query_file_path = sql_file_json.get("edex_source_member_to_badge")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_MemberToBadgeRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_MemberToBadgeRaw_)
            del rows

            # [12] - Subject (meta_data)
            print("** Loading Subject source data...")
            query_file_path = sql_file_json.get("edex_source_subject")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_SubjectRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_SubjectRaw_)
            del rows

            # [13] - Product (meta_data)
            print("** Loading Product source data...")
            query_file_path = sql_file_json.get("edex_source_product")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_ProductRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_ProductRaw_)
            del rows

            # [14] - Comments (meta_data)
            print("** Loading Comments source data...")
            query_file_path = sql_file_json.get("edex_source_comment")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_CommentRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_CommentRaw_)
            del rows

            # [15] - Discussion (meta_data)
            print("** Loading Discussions source data...")
            query_file_path = sql_file_json.get("edex_source_discussion")
            query = open(query_file_path, "r", encoding="utf-8").read()

            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_DiscussionRaw_ = pd.DataFrame(rows, columns = column_names, index = None)
            listSourceDataDataFrame_.append(df_DiscussionRaw_)
            del rows
    finally:
        dbConnection.close()
    tunnel.stop() # stop the tunnel
    print("(EdEx) Source data load finished...", datetime.now())
    return listSourceDataDataFrame_ # df_emailMembersK12lRaw_, df_emailMembersHEDRaw_


def get_source_school_type_codes_raw():
    return listSourceData[0]

def get_source_academi_level_codes_raw():
    return listSourceData[1]

def get_source_members_raw():
    return listSourceData[2]

def get_source_enrollment_raw():
    return listSourceData[3]

def get_source_course_raw():
    return listSourceData[4]

def get_source_resource_raw():
    return listSourceData[5]

def get_source_static_page_raw():
    return listSourceData[6]

def get_source_rating_raw():
    return listSourceData[7]

def get_source_favorite_raw():
    return listSourceData[8]

def get_source_play_list_raw():
    return listSourceData[9]

def get_source_play_list_item_raw():
    return listSourceData[10]

def get_source_member_to_badge_raw():
    return listSourceData[11]

def get_source_subject_raw():
    return listSourceData[12]

def get_source_product_raw():
    return listSourceData[13]

def get_source_comment_raw():
    return listSourceData[14]

def get_source_discussion_raw():
    return listSourceData[15]

# ## Start
listSourceData = get_source_data_from_edex_aws()
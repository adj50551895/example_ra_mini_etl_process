import pymysql
import pandas as pd
from datetime import datetime
import json


# Load local database connetion params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")


def get_dim_data():
    listDimDataDataFrame_ = [] # empty list of Pandas df
    dbConnection = pymysql.connect(
        host=local_host_, user=local_user_,
        password=local_pass_, port=local_port_
    )
    print("(EdEx) Dimension data load started...", datetime.now())
    try:
        with dbConnection.cursor() as cursor:
            # [0] - resourceAcademicLevelRaw
            query = "select t.* from edx.ResourceToAcademicLevel t;" # where isCurrent = '1'
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataResourceAcademicLevel_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataResourceAcademicLevel_)
            del rows
            
            # [1] - memberAcademicLevelRaw
            query = "select t.* from edx.MemberToAcademicLevel t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataMemberAcademicLevel_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataMemberAcademicLevel_)
            del rows
            
            # [2] - memberExperienceRaw - SchoolType
            query = "select t.* from edx.MemberToSchoolType t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataMemberExperience_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataMemberExperience_)
            del rows

            # [3] - academiLevelCodesRaw
            query = "select t.* from edx.AcademicLevel t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataAcademiLevelCodes_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataAcademiLevelCodes_)
            del rows

            # [4] - membersRaw
            query = "select t.* from edx.Member t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataMember_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataMember_)
            del rows

            # [5] - schoolTypeRaw
            query = "select t.* from edx.SchoolType t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataSchoolType_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataSchoolType_)
            del rows

            # [6] - memberGuidMapEdExRaw
            query = "select t.* from hdp.MemberGuidMap t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataMemberGuidMapEd_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataMemberGuidMapEd_)
            del rows

            # [7] - Enrollments
            query = "select t.* from edx.Enrollment t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataEnrollment_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataEnrollment_)
            del rows

            # [8] - Courses
            query = "select t.* from edx.Course t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataCourse_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataCourse_)
            del rows

            # [9] - Resources
            query = "select t.* from edx.Resource t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataResource_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataResource_)
            del rows

            # [10] - courseAcademicLevel
            query = "select t.* from edx.CourseToAcademicLevel t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataCourseAcademicLevel_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataCourseAcademicLevel_)
            del rows

            # [11] - memberSegmentationIncr # ##memberSegmentation_temp
            query = "select t.* from edx.memberSegmentation_temp t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_memberSegmentationTemp_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_memberSegmentationTemp_)
            del rows

            # [12] - staticPage
            query = "select t.* from edx.staticPage t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_staticPage_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_staticPage_)
            del rows

            # [13] - Rating
            query = "select t.* from edx.Rating t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_rating_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_rating_)
            del rows

            # [14] - Favorite
            query = "select t.* from edx.Favorite t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_favorite_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_favorite_)
            del rows

            # [15] - Playlist
            query = "select t.* from edx.Playlist t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_play_list_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_play_list_)
            del rows

            # [16] - PlaylistItem
            query = "select t.* from edx.PlaylistItem t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_play_list_item_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_play_list_item_)
            del rows

            # [17] - MemberToBadge
            query = "select t.* from edx.MemberToBadge t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_member_bagde_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_member_bagde_)
            del rows

            # [18] - Subject
            query = "select t.* from edx.Subject t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_subject_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_subject_)
            del rows

            # [19] - Product
            query = "select t.* from edx.Product t;"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_product_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_product_)
            del rows

            # [20] - resourceProductRaw
            query = "select t.* from edx.ResourceToProduct t;" # where isCurrent = '1'
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataResourceProduct_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataResourceProduct_)
            del rows

            # [21] - resourceSubjectRaw
            query = "select t.* from edx.ResourceToSubject t;" # where isCurrent = '1'
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataResourceSubject_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataResourceSubject_)
            del rows

            # [22] - Comment
            query = "select t.* from edx.Comment t;" # where isCurrent = '1'
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataResourceSubject_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataResourceSubject_)
            del rows

            # [23] - Discussion
            query = "select t.* from edx.Discussion t;" # where isCurrent = '1'
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]
            df_dimDataResourceSubject_ = pd.DataFrame(rows, columns = column_names, index = None)
            listDimDataDataFrame_.append(df_dimDataResourceSubject_)
            del rows
    finally:
        print("(EdEx) Dimension data load finished...", datetime.now())
        dbConnection.close()
    #tunnel.stop() # stop the tunnel
    return listDimDataDataFrame_


# def get_dim_email_member_segmentation():
#     return listDimData[0]    # Rule 1 Emal and Main table

def get_dim_resource_to_academic_level():
    return listDimData[0]

def get_member_to_academic_level():
    return listDimData[1]

# member Expirience
def get_member_to_school_type():
    return listDimData[2]

def get_dim_academic_level_codes():
    return listDimData[3]

def get_dim_members():
    return listDimData[4]

def get_dim_school_type_codes():
    return listDimData[5]

def get_dim_member_guid_map():
    return listDimData[6]

def get_dim_enrollment():
    return listDimData[7]

def get_dim_course():
    return listDimData[8]

def get_dim_resource():
    return listDimData[9]

def get_dim_course_to_academic_level():
    return listDimData[10]

def get_dim_member_segmentation_temp():
    return listDimData[11]

def get_dim_static_page():
    return listDimData[12]

def get_dim_rating():
    return listDimData[13]

def get_dim_favorite():
    return listDimData[14]

def get_dim_play_list():
    return listDimData[15]

def get_play_list_item():
    return listDimData[16]

def get_member_to_badge():
    return listDimData[17]

def get_subject():
    return listDimData[18]

def get_product():
    return listDimData[19]

def get_resource_product():
    return listDimData[20]

def get_resource_subject():
    return listDimData[21]

def get_dim_comment():
    return listDimData[22]

def get_dim_discussion():
    return listDimData[23]
# ## Start
listDimData = get_dim_data()
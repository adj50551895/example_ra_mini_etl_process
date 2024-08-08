import sys
sys.path.append("import")
import pymysql
import pandas as pd
from datetime import datetime
import json

import core.load_source_data_edex as load_source_data_edex
import core.load_dimension_data_edex as load_dimension_data_edex
import core.utils.data_utils as data_utils


# Load local database connetion params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")


# ## Start
## Sources
df_sourceMembers = load_source_data_edex.get_source_members_raw()

df_sourceEnrollment = load_source_data_edex.get_source_enrollment_raw()

df_sourceCourse = load_source_data_edex.get_source_course_raw()

df_sourceResource = load_source_data_edex.get_source_resource_raw()

df_sourceSchoolTypeCodes = load_source_data_edex.get_source_school_type_codes_raw()

df_sourceAcademiLevelCodes = load_source_data_edex.get_source_academi_level_codes_raw()

df_sourceStaticPage = load_source_data_edex.get_source_static_page_raw()

df_sourceRating = load_source_data_edex.get_source_rating_raw()

df_sourceFavorite = load_source_data_edex.get_source_favorite_raw()

df_sourcePlayList = load_source_data_edex.get_source_play_list_raw()

df_sourcePlayListItem = load_source_data_edex.get_source_play_list_item_raw()

df_sourceMemberToBadge = load_source_data_edex.get_source_member_to_badge_raw()

df_sourceSubject = load_source_data_edex.get_source_subject_raw()

df_sourceProduct = load_source_data_edex.get_source_product_raw()

df_sourceComment = load_source_data_edex.get_source_comment_raw()

df_sourceDiscussion = load_source_data_edex.get_source_discussion_raw()


# Parse raw data
print("(EdEx) Parsing CourseToAcademicLevel", datetime.now())
df_sourceCourseAcademicLevelRaw = df_sourceCourse[["id", "academicLevels"]]
df_sourceCourseAcademicLevelRaw.columns = ["courseId", "academicLevels"]
df_sourceCourseAcademicLevelRaw = df_sourceCourseAcademicLevelRaw[(df_sourceCourseAcademicLevelRaw["academicLevels"] != "{}")]
df_sourceCourseAcademicLevelRaw = df_sourceCourseAcademicLevelRaw[(df_sourceCourseAcademicLevelRaw["academicLevels"] != "null")]
df_sourceCourseAcademicLevel = data_utils.create_academic_level_from_df(df_sourceCourseAcademicLevelRaw, "academicLevels", "secondary", "courseId")

print("(EdEx) Parsing ResourceToAcademicLevel secondary", datetime.now())
df_sourceResourceAcademicLevelRaw = df_sourceResource[["id", "academicLevels"]]
df_sourceResourceAcademicLevelRaw.columns = ["resourceId", "academicLevels"]
df_sourceResourceAcademicLevelRaw = df_sourceResourceAcademicLevelRaw[df_sourceResourceAcademicLevelRaw["academicLevels"].str.contains("\"primary\": null, \"secondary\": \[]")==False]
df_sourceResourceAcademicLevelRaw = df_sourceResourceAcademicLevelRaw[df_sourceResourceAcademicLevelRaw["academicLevels"].str.contains("\"primary\": \"\", \"secondary\": \[]")==False]
df_sourceResourceAcademicLevelPrm = data_utils.create_academic_level_from_df(df_sourceResourceAcademicLevelRaw, "academicLevels", "secondary", "resourceId")
df_sourceResourceAcademicLevelPrm["layer"] = "secondary"
df_sourceResourceAcademicLevelPrm = df_sourceResourceAcademicLevelPrm.drop_duplicates()

print("(EdEx) Parsing ResourceToAcademicLevel primary", datetime.now())
df_sourceResourceAcademicLevelRaw = df_sourceResource[["id", "academicLevels"]]
df_sourceResourceAcademicLevelRaw.columns = ["resourceId", "academicLevels"]
df_sourceResourceAcademicLevelRaw = df_sourceResourceAcademicLevelRaw[df_sourceResourceAcademicLevelRaw["academicLevels"].str.contains("\"primary\": null, \"secondary\": \[]")==False]
df_sourceResourceAcademicLevelRaw = df_sourceResourceAcademicLevelRaw[df_sourceResourceAcademicLevelRaw["academicLevels"].str.contains("\"primary\": \"\", \"secondary\": \[]")==False]
df_sourceResourceAcademicLevelRaw = df_sourceResourceAcademicLevelRaw[df_sourceResourceAcademicLevelRaw["academicLevels"].str.contains("\"primary\": \"\",")==False]
df_sourceResourceAcademicLevelSec = data_utils.create_academic_level_from_df(df_sourceResourceAcademicLevelRaw, "academicLevels", "primary", "resourceId")
df_sourceResourceAcademicLevelSec["layer"] = "primary"
df_sourceResourceAcademicLevelSec = df_sourceResourceAcademicLevelSec.drop_duplicates()
df_sourceResourceAcademicLevel = pd.concat([df_sourceResourceAcademicLevelPrm, df_sourceResourceAcademicLevelSec])


print("(EdEx) Parsing MemberToAcademicLevel", datetime.now())
df_sourceMemberAcademicLevelRaw = df_sourceMembers[["id","interests"]]
df_sourceMemberAcademicLevelRaw.columns = ["memberId", "academicLevels"]
df_sourceMemberAcademicLevelRaw = df_sourceMemberAcademicLevelRaw[df_sourceMemberAcademicLevelRaw["academicLevels"].str.contains("\"academicLevels\": \[]")==False]
df_sourceMemberAcademicLevel = data_utils.create_academic_level_from_df(df_sourceMemberAcademicLevelRaw, "academicLevels", "academicLevels", "memberId")
df_sourceMemberAcademicLevel = df_sourceMemberAcademicLevel.drop_duplicates()

print("(EdEx) Parsing MemberExperience", datetime.now())
df_sourceMemberExperienceRaw = df_sourceMembers[["id","experience"]]
df_sourceMemberExperienceRaw.columns = ["memberId", "experience"]
df_sourceMemberExperienceRaw = df_sourceMemberExperienceRaw.loc[df_sourceMemberExperienceRaw["experience"]!="[]"]
df_sourceMemberExperience = data_utils.create_df_from_list_of_dict(df_sourceMemberExperienceRaw, "experience", "schoolTypeID", "memberId")
df_sourceMemberExperience = df_sourceMemberExperience.drop_duplicates()


print("(EdEx) Parsing ResourceToProduct secondary", datetime.now())
df_sourceResourceProductRaw = df_sourceResource[["id", "products"]]
df_sourceResourceProductRaw.columns = ["resourceId", "products"]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[df_sourceResourceProductRaw["products"].str.contains("\"primary\": null, \"secondary\": \[]")==False]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[df_sourceResourceProductRaw["products"].str.contains("\"primary\": \"\", \"secondary\": \[]")==False]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[(df_sourceResourceProductRaw["products"] != "{}")]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[(df_sourceResourceProductRaw["products"] != "null")]
df_sourceResourceProductSec = data_utils.create_element_from_df(df_sourceResourceProductRaw, "products", "secondary", "resourceId")
df_sourceResourceProductSec["layer"] = "secondary"
df_sourceResourceProductSec = df_sourceResourceProductSec.drop_duplicates()


print("(EdEx) Parsing ResourceToProduct primary", datetime.now())
del df_sourceResourceProductRaw
df_sourceResourceProductRaw = df_sourceResource[["id", "products"]]
df_sourceResourceProductRaw.columns = ["resourceId", "products"]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[df_sourceResourceProductRaw["products"].str.contains("\"primary\": null, \"secondary\": \[]")==False]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[df_sourceResourceProductRaw["products"].str.contains("\"primary\": \"\", \"secondary\": \[]")==False]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[df_sourceResourceProductRaw["products"].str.contains("\"primary\": \"\",")==False]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[df_sourceResourceProductRaw["products"].str.match("{\"secondary\": \[]}")==False]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[(df_sourceResourceProductRaw["products"] != "{}")]
df_sourceResourceProductRaw = df_sourceResourceProductRaw[(df_sourceResourceProductRaw["products"] != "null")]
df_sourceResourceProductPrm = data_utils.create_element_from_df(df_sourceResourceProductRaw, "products", "primary", "resourceId")
df_sourceResourceProductPrm["layer"] = "primary"
df_sourceResourceProductPrm = df_sourceResourceProductPrm.drop_duplicates()
df_sourceResourceProduct = pd.concat([df_sourceResourceProductPrm, df_sourceResourceProductSec])
df_sourceResourceProduct = df_sourceResourceProduct.drop_duplicates()


print("(EdEx) Parsing ResourceToSubject secondary", datetime.now())
df_sourceResourceSubjectRaw = df_sourceResource[["id", "subjects"]]
df_sourceResourceSubjectRaw.columns = ["resourceId", "subjects"]
df_sourceResourceSubjectRaw = df_sourceResourceSubjectRaw[df_sourceResourceSubjectRaw["subjects"].str.contains("\"primary\": null, \"secondary\": \[]")==False]
df_sourceResourceSubjectRaw = df_sourceResourceSubjectRaw[df_sourceResourceSubjectRaw["subjects"].str.contains("\"primary\": \"\", \"secondary\": \[]")==False]
df_sourceResourceSubjectRaw = df_sourceResourceSubjectRaw[(df_sourceResourceSubjectRaw["subjects"] != "{}")]
df_sourceResourceSubjectRaw = df_sourceResourceSubjectRaw[(df_sourceResourceSubjectRaw["subjects"] != "null")]
df_sourceResourceSubjectSec = data_utils.create_element_from_df(df_sourceResourceSubjectRaw, "subjects", "secondary", "resourceId")
df_sourceResourceSubjectSec["layer"] = "secondary"
df_sourceResourceSubjectSec = df_sourceResourceSubjectSec.drop_duplicates()


print("(EdEx) Parsing ResourceToSubject primary", datetime.now())
del df_sourceResourceSubjectRaw
df_sourceResourceSubjectRaw = df_sourceResource[["id", "subjects"]]
df_sourceResourceSubjectRaw.columns = ["resourceId", "subjects"]
df_sourceResourceSubjectRaw = df_sourceResourceSubjectRaw[df_sourceResourceSubjectRaw["subjects"].str.contains("\"primary\": null, \"secondary\": \[]")==False]
df_sourceResourceSubjectRaw = df_sourceResourceSubjectRaw[df_sourceResourceSubjectRaw["subjects"].str.contains("\"primary\": \"\", \"secondary\": \[]")==False]
df_sourceResourceSubjectRaw = df_sourceResourceSubjectRaw[(df_sourceResourceSubjectRaw["subjects"] != "{}")]
df_sourceResourceSubjectRaw = df_sourceResourceSubjectRaw[(df_sourceResourceSubjectRaw["subjects"] != "null")]
df_sourceResourceSubjectRaw = df_sourceResourceSubjectRaw[df_sourceResourceSubjectRaw["subjects"].str.contains("\"primary\": \"\",")==False]
df_sourceResourceSubjectPrm = data_utils.create_element_from_df(df_sourceResourceSubjectRaw, "subjects", "primary", "resourceId")
df_sourceResourceSubjectPrm["layer"] = "primary"
df_sourceResourceSubject = pd.concat([df_sourceResourceSubjectPrm, df_sourceResourceSubjectSec])
df_sourceResourceSubject = df_sourceResourceSubject.drop_duplicates()


## Dimensions
# edx.ResourceToAcademicLevel
df_dimResourceAcademicLevel = load_dimension_data_edex.get_dim_resource_to_academic_level()

# edx.MemberToAcademicLevel
df_dimMemberAcademicLevel = load_dimension_data_edex.get_member_to_academic_level()

# edx.MemberToSchoolType
df_dimMemberToSchoolType = load_dimension_data_edex.get_member_to_school_type()

# edx.AcademicLevel
df_dimAcademicLevelCodes = load_dimension_data_edex.get_dim_academic_level_codes()

# edx.Member
df_dimMembers = load_dimension_data_edex.get_dim_members();

# edx.SchoolTypeDim
df_dimSchoolTypeCodesRaw = load_dimension_data_edex.get_dim_school_type_codes()

# edx.edexSegmentationGuidsExt
df_dimMemberGuidMap = load_dimension_data_edex.get_dim_member_guid_map()

# edx.Enrollment
df_dimEnrollment = load_dimension_data_edex.get_dim_enrollment()

# edx.Course
df_dimCourse = load_dimension_data_edex.get_dim_course()

# edx.Resource
df_dimResource = load_dimension_data_edex.get_dim_resource()

# edx.CourseToAcademicLevel
df_dimCourseToAcademicLevel = load_dimension_data_edex.get_dim_course_to_academic_level()

# edx.StaticPage
df_dimStaticPage = load_dimension_data_edex.get_dim_static_page()

# edx.Rating
df_dimRating = load_dimension_data_edex.get_dim_rating()

# edx.Favorite
df_dimFavorite = load_dimension_data_edex.get_dim_favorite()

# edx.Playlist
df_dimPlayList = load_dimension_data_edex.get_dim_play_list()

# edx.PlaylistItem
df_dimPlayListItem = load_dimension_data_edex.get_play_list_item()

# edx.MemberToBadge
df_dimMemberToBadge = load_dimension_data_edex.get_member_to_badge()

# edx.Subject
df_dimSubject = load_dimension_data_edex.get_subject()

# edx.Product
df_dimProduct = load_dimension_data_edex.get_product()

# edx.ResourceToProduct
df_dimResourceProduct = load_dimension_data_edex.get_resource_product()

# edx.ResourceToSubject
df_dimResourceSubject = load_dimension_data_edex.get_resource_subject()

# edx.Comment
df_dimComment = load_dimension_data_edex.get_dim_comment()

# edx.ResourceToSubject
df_dimDiscussion = load_dimension_data_edex.get_dim_discussion()


# ## UPDATE, INSERT, DELETE
dbConnection = pymysql.connect(
    host=local_host_, user=local_user_,
    password=local_pass_, port=local_port_
)
try:
    # Resource, Academic Level
    table = "edx.ResourceToAcademicLevel"
    print(table+" process started...", datetime.now())
    #columnKey = ["resourceId", "academicLevels", "layer"]
    columnKey = list(df_sourceResourceAcademicLevel.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceResourceAcademicLevel, df_dimResourceAcademicLevel, columnKey)

    if not df_merge_insert.empty:
        data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, columnKey) # ##Comment on the first launch   # ide columnKey

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.ResourceToAcademicLevel_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())


    # Member, Academic Level
    table = "edx.MemberToAcademicLevel"
    print(table+" process started...", datetime.now())
    #columnKey = ["memberId", "academicLevels"]
    columnKey = list(df_sourceMemberAcademicLevel.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceMemberAcademicLevel, df_dimMemberAcademicLevel, columnKey)

    if not df_merge_insert.empty:
        data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, columnKey) # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.MemberToAcademicLevel_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())


    # Member, Experience
    table = "edx.MemberToSchoolType"
    print(table+" process started...", datetime.now())
    #columnKey = ["memberId", "schoolTypeID"]
    columnKey = list(df_sourceMemberExperience.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceMemberExperience, df_dimMemberToSchoolType, columnKey)

    if not df_merge_insert.empty:
        data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, columnKey) # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.MemberToSchoolType_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())


    # Academic Level Codes
    table = "edx.AcademicLevel"
    print(table+" process started...", datetime.now())
    #columnKey = ["id", "i18nCategory", "i18nLabel", "urlLabel", "eduLevel"]
    columnKey = list(df_sourceAcademiLevelCodes.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceAcademiLevelCodes, df_dimAcademicLevelCodes, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey) # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.AcademicLevel_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())


    # Members
    table = "edx.Member"
    print(table+" process started...", datetime.now())
    # ## 02.02.2023 columnKey = ["id", "adobeGUID", "imsUserId", "imsAuthId", "firstName", "lastName", "email", "vanityURL", "jobTitle", "city", "regionID", "interests", "experience", "countryCode", "status", "educationSegmentID", "settings", "reputationPoints", "schoolEmail", "schoolEmailVerified", "schoolEmailOrgID", "createdAt"]
    columnKey = list(df_sourceMembers.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceMembers, df_dimMembers, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey) # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Member_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)

        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # MemberGuidMap, nema potrebe za dfProcessStageData, mora da ide iza membera jer se koristi df_merge_insert, izvedena tabela
    table = "hdp.MemberGuidMap"
    print(table+" process started...", datetime.now())
    columnKey = ["id", "adobeGUID", "imsAuthId", "status"] # column key from member
    df_merge_guid_insert = df_merge_insert[columnKey]
    if not df_merge_guid_insert.empty:
        memberGuidMapColumnKey = ["memberId", "userGuid", "status"]
        df_merge_guid_insert["imsAuthIdTmp"] = df_merge_guid_insert["imsAuthId"].str.split("@").str[0]
        df_merge_guid_insert["userGuid"] = df_merge_guid_insert.apply(lambda x: x["adobeGUID"] if x["adobeGUID"] != None else x["imsAuthIdTmp"], axis=1)
        df_merge_guid_insert = df_merge_guid_insert[["id", "userGuid", "status"]]
        df_merge_guid_insert.columns = memberGuidMapColumnKey #["memberId", "userGuid", "status"]
        df_merge_guid_insert = df_merge_guid_insert.drop_duplicates()

        df_dimMemberGuidMap = df_dimMemberGuidMap[memberGuidMapColumnKey]
        df_merge_guid_insert_1 = pd.merge(df_merge_guid_insert, df_dimMemberGuidMap, how="left", on = memberGuidMapColumnKey, indicator=True)
        df_merge_guid_insert_1 = df_merge_guid_insert_1[df_merge_guid_insert_1["_merge"].eq("left_only")]
        df_merge_guid_insert_1 = df_merge_guid_insert_1[memberGuidMapColumnKey]

        if not df_merge_guid_insert_1.empty:
            primaryKey = ["memberId"]
            data_utils.update_dim_table_close_dateto_by_ids(df_merge_guid_insert_1, dbConnection, table, primaryKey) # ##Comment on the first launch

            df_merge_guid_insert_1["dateFrom"] = pd.Timestamp.now()
            df_merge_guid_insert_1["dateTo"] = ""
            df_merge_guid_insert_1["isCurrent"] = "1"

            data_utils.insert_df_data_to_table(df_merge_guid_insert_1, dbConnection, table)

            tableHist = "hdp.MemberGuidMap_hist"
            data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
            data_utils.delete_hist_data_from_main_table(dbConnection, table)

            dbConnection.commit()
            del df_merge_guid_insert_1, memberGuidMapColumnKey
            print(table+" processed...", datetime.now())


    # School Types Codes
    table = "edx.SchoolType"
    print(table+" process started...", datetime.now())
    #columnKey = ["id", "i18nLabel", "schoolCategory"]
    columnKey = list(df_sourceSchoolTypeCodes.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceSchoolTypeCodes, df_dimSchoolTypeCodesRaw, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)  # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.SchoolType_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)

        dbConnection.commit()
    print(table+" processed...", datetime.now())


    # Enrollment
    table = "edx.Enrollment"
    print(table+" process started...", datetime.now())
    #columnKey = ["memberID", "courseID", "status", "reviewComment", "progress", "completedAt", "startedAt", "lastActivityAt", "lastReviewedAt", "lastReviewedBy", "createdAt", "updatedAt", "requestID", "learningJournalURL"]
    columnKey = list(df_sourceEnrollment.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceEnrollment, df_dimEnrollment, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["memberID", "courseID"]
        data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, primaryKey)  # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Enrollment_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())


    # Course
    table = "edx.Course"
    print(table+" process started...", datetime.now())
    df_dimCourse["graduationAt"]= pd.to_datetime(df_dimCourse["graduationAt"])
    #columnKey = ["id", "vanityURL", "siteID", "title", "shortDescription", "description", "type", "courseTypeID", "difficulty", "assets", "settings", "tags", "theme", "workshops", "contentStandards", "academicLevels", "products", "subjects", "badges", "forumID", "educators", "relatedContent", "status", "enrollmentOpensAt", "startsAt", "enrollmentClosesAt", "publishAt", "closesAt", "forumClosesAt", "graduationAt", "createdAt", "createdBy", "updatedAt", "updatedBy", "requestID", "credlyBadges"]
    columnKey = list(df_sourceCourse.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceCourse, df_dimCourse, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["id"]
        #data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, primaryKey) #23.11.2022
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)  # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Course_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())


    # Resourse
    table = "edx.Resource"
    print(table+" process started...", datetime.now())
    #columnKey = ["id", "siteID", "title", "shortDescription", "description", "products", "subjects", "academicLevels", "tags", "internalTags", "vanityURL", "SEOUrl", "heroImage", "type", "status", "subscribed", "createdAt", "createdBy", "updatedAt", "updatedBy", "publishedAt", "requestID", "copyLicenses", "timing", "ranking", "technicalExpertise", "standards", "links", "components", "settings", "public", "locked"]
    columnKey = list(df_sourceResource.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceResource, df_dimResource, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["id"]
        #data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, primaryKey) #23.11.2022
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)  # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Resource_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())


    # CourseToAcademicLevel
    table = "edx.CourseToAcademicLevel"
    print(table+" process started...", datetime.now())
    #columnKey = ["courseId", "academicLevels"]
    columnKey = list(df_sourceCourseAcademicLevel.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceCourseAcademicLevel, df_dimCourseToAcademicLevel, columnKey)

    if not df_merge_insert.empty:
        data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, columnKey)  # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.CourseToAcademicLevel_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # staticPage
    table = "edx.StaticPage"
    print(table+" process started...", datetime.now())
    #columnKey = ["id"]
    columnKey = list(df_sourceStaticPage.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceStaticPage, df_dimStaticPage, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)  # ##Comment on the first launch

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.staticPage_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # Rating
    table = "edx.Rating"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourceRating.columns)
    #df_dimRating["updatedAt"] = pd.to_datetime(df_dimRating["updatedAt"]) # 08.02.2023 add because is null object in database
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceRating, df_dimRating, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Rating_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # Favorite
    table = "edx.Favorite"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourceFavorite.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceFavorite, df_dimFavorite, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Favorite_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # PlayList
    table = "edx.Playlist"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourcePlayList.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourcePlayList, df_dimPlayList, columnKey)

    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Playlist_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # PlayListItem
    table = "edx.PlayListItem"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourcePlayListItem.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourcePlayListItem, df_dimPlayListItem, columnKey) #    
    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.PlayListItem_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # MemberToBadge
    table = "edx.MemberToBadge"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourceMemberToBadge.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceMemberToBadge, df_dimMemberToBadge, columnKey) #    
    if not df_merge_insert.empty:
        primaryKey = ["memberBadgeID"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.MemberToBadge_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # Subject
    table = "edx.Subject"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourceSubject.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceSubject, df_dimSubject, columnKey) #    
    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Subject_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # Product
    table = "edx.Product"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourceProduct.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceProduct, df_dimProduct, columnKey) #    
    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Product_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # ResourceToProduct
    table = "edx.ResourceToProduct"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourceResourceProduct.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceResourceProduct, df_dimResourceProduct, columnKey)

    if not df_merge_insert.empty:
        data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, columnKey) # ##Comment on the first launch   # ide columnKey

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.ResourceToProduct_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # ResourceToSubject
    table = "edx.ResourceToSubject"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourceResourceSubject.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceResourceSubject, df_dimResourceSubject, columnKey)

    if not df_merge_insert.empty:
        data_utils.update_dim_table_close_date_to(df_merge_insert, dbConnection, table, columnKey) # ##Comment on the first launch   # ide columnKey

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.ResourceToSubject_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # Comments
    table = "edx.Comment"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourceComment.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceComment, df_dimComment, columnKey)
    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Comment_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    # Discussion
    table = "edx.Discussion"
    print(table+" process started...", datetime.now())
    columnKey = list(df_sourceDiscussion.columns)
    df_merge_insert = data_utils.df_process_stage_data_insert(df_sourceDiscussion, df_dimDiscussion, columnKey)
    if not df_merge_insert.empty:
        primaryKey = ["id"]
        data_utils.update_dim_table_close_dateto_by_ids(df_merge_insert, dbConnection, table, primaryKey)

        df_merge_insert["dateFrom"] = pd.Timestamp.now()
        df_merge_insert["dateTo"] = ""
        df_merge_insert["isCurrent"] = "1"
        data_utils.insert_df_data_to_table(df_merge_insert, dbConnection, table)

        tableHist = "edx.Discussion_hist"
        data_utils.insert_hist_data_to_hist_table(dbConnection, table, tableHist)
        data_utils.delete_hist_data_from_main_table(dbConnection, table)
        dbConnection.commit()
    print(table+" processed...", datetime.now())

    del table, tableHist, primaryKey, columnKey
finally:
    dbConnection.close()

print("(EdEx) Dimension process finished...", datetime.now())

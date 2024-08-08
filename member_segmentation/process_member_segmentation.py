import sys
sys.path.append("import") # because load_dimension_data_table_v1 is in another directory
import pandas as pd
import pymysql
import core.load_dimension_data_edex as load_dimension_data_edex
import core.utils.data_utils as data_utils

from datetime import date
from datetime import datetime
from core.load_dimension_data_elasticsearch import get_member_edu_level
import core.utils.load_dimension_data_table as load_dimension_data_table
import json
#from LoadDimensionDataEdex_v1 import getDimMemberSegmentation


## # Note: Rule 10, Rule 12 are implemneted in "process_member_segmentation_table.sql"


# Load local database connetion params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")

tableMemberSegmentationTemp = "edx.MemberSegmentation_temp"

# RULE 1
print("Rule 1 started...", datetime.now())
emailMembers_query ="""
SELECT distinct m.id as memberId, 'K12' as class_tmp, 'K12' as class, '1' as rule, sysdate() as createdAt
FROM edx.Member m
WHERE m.status = 'active'
AND (UPPER(SUBSTRING(m.email, POSITION('@' IN m.email))) like '%.K12.%' 
     or UPPER(SUBSTRING(m.email, POSITION('@' IN m.email))) like '%.USD.%' 
     or UPPER(SUBSTRING(m.email, POSITION('@' IN m.email))) like '%.ISD.%' 
     or UPPER(m.email) like '%EDU.PS')
AND LOWER(m.email) not like '%student%'
UNION ALL
SELECT distinct m.id as memberId, 'HED' as class_tmp, 'HED' as class, '1' as rule, sysdate() as createdAt
FROM edx.Member m
WHERE m.status = 'active'
AND (UPPER(SUBSTRING(m.email, POSITION('@' IN m.email), LENGTH(m.email))) like '%22.EDU' 
     or UPPER(SUBSTRING(m.email, POSITION('@' IN m.email), LENGTH(m.email))) like '%COLLEGE%' 
     or UPPER(SUBSTRING(m.email, POSITION('@' IN m.email), LENGTH(m.email))) like '%UNIVERSITY%')
AND UPPER(m.email) not like '%student%'
AND UPPER(SUBSTRING(m.email, POSITION('@' IN m.email), LENGTH(m.email))) not like '%EDU.PS';
"""
print("Rule 1 finished...", datetime.now())
# //RULE 1

# RULE 11 24.12.2022
print("Rule 11 started...", datetime.now())
emailDomain_query = """
select t1.memberId,
       t1.class as class_tmp,
       max(t1.class) as class,
       '11' as rule,
       sysdate() as createdAt
from (
select distinct m.id as memberId, t.school_class as class
from edx.Member m
join edx.school_domain t on t.domain = TRIM(LOWER(SUBSTRING_INDEX(m.email, '@', - 1)))
where t.school_class is not null
and t.school_type = 'school'
union
select distinct m.id as memberId, t.school_class as class
from edx.Member m
join edx.school_domain t on t.domain = TRIM(LOWER(SUBSTRING_INDEX(m.schoolemail, '@', - 1)))
where t.school_class is not null
and t.school_type = 'school'
) t1
group by t1.memberId,
         t1.class;
"""
print("Rule 11 finished...", datetime.now())
# // RULE 11

# RULE 2
print("Rule 2 started...", datetime.now())
df_memberEduLevel_1 = get_member_edu_level()

df_memberEduLevel_1 = df_memberEduLevel_1.drop(columns=["layer"])
df_memberEduLevel = df_memberEduLevel_1.groupby(["memberId", "eduLevel"]).agg(cnt_memberEduLevel = ('cnt_memberEduLevel','sum'), sum_EventCount = ('sum_EventCount','sum')).reset_index()


df_memberEduLevelK12 = df_memberEduLevel[(df_memberEduLevel["eduLevel"] == "K12")]
df_memberEduLevelHED = df_memberEduLevel[(df_memberEduLevel["eduLevel"] == "HED")]

# samo memberi koji imaju Created\Viewed\Downloaded K12 ili HED resurse
df_resourceK12Only = df_memberEduLevelK12[~df_memberEduLevel.memberId.isin(df_memberEduLevelHED.memberId)]
df_resourceHEDOnly = df_memberEduLevelHED[~df_memberEduLevel.memberId.isin(df_memberEduLevelK12.memberId)]

# possible members
df_possibleK12HED = pd.merge(left = df_memberEduLevelK12, right = df_memberEduLevelHED, how = "inner", left_on = "memberId", right_on = "memberId")
df_possibleK12HED["sum_EventCountRateK12"] = df_possibleK12HED["sum_EventCount_x"]/df_possibleK12HED["sum_EventCount_y"]
df_possibleK12HED["sum_EventCountRateHED"] = df_possibleK12HED["sum_EventCount_y"]/df_possibleK12HED["sum_EventCount_x"]
#df_possibleK12HED["sum_EventCountRateK12"] = df_possibleK12HED["cnt_memberEduLevel_x"]/df_possibleK12HED["cnt_memberEduLevel_y"]
#df_possibleK12HED["sum_EventCountRateHED"] = df_possibleK12HED["cnt_memberEduLevel_y"]/df_possibleK12HED["cnt_memberEduLevel_x"]

df_possibleK12 = df_possibleK12HED[(df_possibleK12HED["sum_EventCountRateK12"]>1.5)]
df_possibleHED = df_possibleK12HED[(df_possibleK12HED["sum_EventCountRateHED"]>1.5)]

df_possibleK12 = df_possibleK12[["memberId", "eduLevel_x", "eduLevel_x"]]
df_possibleK12.columns = ["memberId", "class_tmp", "class"]
df_possibleK12["class_tmp"] = "Possible K12"
df_possibleK12["rule"] = "2"
df_possibleK12["createdAt"] = date.today()

df_possibleHED = df_possibleHED[["memberId", "eduLevel_y", "eduLevel_y"]]
df_possibleHED.columns = ["memberId", "class_tmp", "class"]
df_possibleHED["class_tmp"] = "Possible HED"
df_possibleHED["rule"] = "2"
df_possibleHED["createdAt"] = date.today()

df_resourceK12Only = df_resourceK12Only[["memberId", "eduLevel", "eduLevel"]]
df_resourceK12Only.columns = ["memberId", "class_tmp", "class"]
df_resourceK12Only["rule"] = "2"
df_resourceK12Only["createdAt"] = date.today()

df_resourceHEDOnly = df_resourceHEDOnly[["memberId", "eduLevel", "eduLevel"]]
df_resourceHEDOnly.columns = ["memberId", "class_tmp", "class"]
df_resourceHEDOnly["rule"] = "2"
df_resourceHEDOnly["createdAt"] = date.today()

del df_memberEduLevel_1, df_memberEduLevel, df_memberEduLevelK12, df_memberEduLevelHED, df_possibleK12HED

# Additional, based on Resource and ResourceToAcademicLevel
df_resourceDim = load_dimension_data_edex.get_dim_resource()
df_resourceToAcademicLevel = load_dimension_data_edex.get_dim_resource_to_academic_level()
df_academicLevelDim = load_dimension_data_edex.get_dim_academic_level_codes()

df_resurceAcademicLevel = pd.merge(left = df_resourceToAcademicLevel, right = df_resourceDim, how = "inner", left_on = "resourceId", right_on = "id")
df_resurceAcademicLevel = pd.merge(left = df_resurceAcademicLevel, right = df_academicLevelDim, how = "inner", left_on = "academicLevels_x", right_on = "id")

df_resurceAcademicLevel = df_resurceAcademicLevel[["createdBy", "eduLevel"]]
df_resurceAcademicLevel = df_resurceAcademicLevel.groupby(["createdBy", "eduLevel"]).size().reset_index(name="cnt_")
df_resurceAcademicLevel.columns = ["memberId", "eduLevel", "cnt_"]

df_resurceAcademicLevel = df_resurceAcademicLevel[(df_resurceAcademicLevel["eduLevel"] == "K12") | (df_resurceAcademicLevel["eduLevel"] == "HED")]

df_resurceAcademicLevelK12 = df_resurceAcademicLevel[(df_resurceAcademicLevel["eduLevel"] == "K12")]
df_resurceAcademicLevelHED = df_resurceAcademicLevel[(df_resurceAcademicLevel["eduLevel"] == "HED")]

# K12 only
df_resurceAcademicLevelK12Only = df_resurceAcademicLevel[~df_resurceAcademicLevel.memberId.isin(df_resurceAcademicLevelHED.memberId)]
df_resurceAcademicLevelK12Only = df_resurceAcademicLevelK12Only[["memberId", "eduLevel", "eduLevel"]]
df_resurceAcademicLevelK12Only.columns = ["memberId", "class_tmp", "class"]
df_resurceAcademicLevelK12Only["rule"] = "2"
df_resurceAcademicLevelK12Only["createdAt"] = date.today()

# HED only
df_resurceAcademicLevelHEDOnly = df_resurceAcademicLevel[~df_resurceAcademicLevel.memberId.isin(df_resurceAcademicLevelK12.memberId)]
df_resurceAcademicLevelHEDOnly = df_resurceAcademicLevelHEDOnly[["memberId", "eduLevel", "eduLevel"]]
df_resurceAcademicLevelHEDOnly.columns = ["memberId", "class_tmp", "class"]
df_resurceAcademicLevelHEDOnly["rule"] = "2"
df_resurceAcademicLevelHEDOnly["createdAt"] = date.today()

df_resurceAcademicLevelPossible =  pd.merge(left = df_resurceAcademicLevel, right = df_resurceAcademicLevel, how = "inner", left_on = "memberId", right_on = "memberId")
# more K12 than HED
df_resurceAcademicLevelPossibleK12 = df_resurceAcademicLevelPossible[(df_resurceAcademicLevelPossible["eduLevel_x"] == "K12") & (df_resurceAcademicLevelPossible["eduLevel_y"] == "HED") & (df_resurceAcademicLevelPossible["cnt__x"] > df_resurceAcademicLevelPossible["cnt__y"])]
df_resurceAcademicLevelPossibleK12 = df_resurceAcademicLevelPossibleK12[["memberId", "eduLevel_x", "eduLevel_x"]]
df_resurceAcademicLevelPossibleK12.columns = ["memberId", "class_tmp", "class"]
df_resurceAcademicLevelPossibleK12["class_tmp"]= "Possible K12"
df_resurceAcademicLevelPossibleK12["rule"] = "2"
df_resurceAcademicLevelPossibleK12["createdAt"] = date.today()

# more HED than K12
df_resurceAcademicLevelPossibleHED = df_resurceAcademicLevelPossible[(df_resurceAcademicLevelPossible["eduLevel_x"] == "HED") & (df_resurceAcademicLevelPossible["eduLevel_y"] == "K12") & (df_resurceAcademicLevelPossible["cnt__x"] > df_resurceAcademicLevelPossible["cnt__y"])]
df_resurceAcademicLevelPossibleHED = df_resurceAcademicLevelPossibleHED[["memberId", "eduLevel_x", "eduLevel_x"]]
df_resurceAcademicLevelPossibleHED.columns = ["memberId", "class_tmp", "class"]
df_resurceAcademicLevelPossibleHED["class_tmp"]= "Possible HED"
df_resurceAcademicLevelPossibleHED["rule"] = "2"
df_resurceAcademicLevelPossibleHED["createdAt"] = date.today()


df_rule2 = pd.concat([df_possibleK12, df_possibleHED, df_resourceK12Only, df_resourceHEDOnly, df_resurceAcademicLevelK12Only, df_resurceAcademicLevelHEDOnly, df_resurceAcademicLevelPossibleK12, df_resurceAcademicLevelPossibleHED], join="outer", ignore_index=True)
df_rule2 = df_rule2.drop_duplicates()

del df_resourceDim, df_resourceToAcademicLevel, df_academicLevelDim, df_resurceAcademicLevel, df_resurceAcademicLevelPossible, df_resurceAcademicLevelK12, df_resurceAcademicLevelHED
del df_possibleK12, df_possibleHED, df_resourceK12Only, df_resourceHEDOnly, df_resurceAcademicLevelK12Only, df_resurceAcademicLevelHEDOnly, df_resurceAcademicLevelPossibleK12, df_resurceAcademicLevelPossibleHED

print("Rule 2 finished...", datetime.now())
# //RULE2


# RULE 3
print("Rule 3 started...", datetime.now())
df_academicLevelDim = load_dimension_data_edex.get_dim_academic_level_codes()
df_memberToAcademicLevelDim = load_dimension_data_edex.get_member_to_academic_level()

df = pd.merge(left = df_memberToAcademicLevelDim, right = df_academicLevelDim, how = "inner", left_on = "academicLevels", right_on = "id")

# group by "memberId", count(*)
df_1 = df.groupby(["memberId"]).size().reset_index(name="counts_memberID")

# group by "memberId", "eduLevel", count(*)
df_2 = df.groupby(["memberId","eduLevel"]).size().reset_index(name="counts_memberID_eduLevel")


df_3 = pd.merge(left = df_1, right = df_2, how = "inner", on = "memberId")
df_3["eduRate"] = df_3["counts_memberID_eduLevel"]/df_3["counts_memberID"]

df_tmp = df_3[df_3["eduRate"]==1]
df_tmp["class_tmp"] = df_tmp["eduLevel"]
df_tmp["class"] = df_tmp["eduLevel"]
df_tmp["rule"] = "3"
df_tmp["createdAt"] = date.today()

df_segStudentsAgeLevel_1 = df_tmp[["memberId", "class_tmp", "class", "rule", "createdAt"]] # ovo insertovati

df_tmp = df_3[(df_3["eduRate"]>0.5) & (df_3["eduRate"] < 1)]
df_tmp["class_tmp"] = "Possible"
df_tmp["class"] = df_tmp["eduLevel"]
df_tmp["rule"] = "3"
df_tmp["createdAt"] = date.today()

df_segStudentsAgeLevel_2 = df_tmp[["memberId", "class_tmp", "class", "rule", "createdAt"]] # ovo insertovati

df_rule3 = pd.concat([df_segStudentsAgeLevel_1, df_segStudentsAgeLevel_2], join="outer", ignore_index=True)
df_rule3 = df_rule3.drop_duplicates()

# df_tmp is used in RULE 4
del df, df_1, df_2, df_3, df_tmp, df_segStudentsAgeLevel_1, df_segStudentsAgeLevel_2
print("Rule 3 finished...", datetime.now())
# //RULE 3


# RULE 4
print("Rule 4 started...", datetime.now())
df_memberToSchoolTypeDim = load_dimension_data_edex.get_member_to_school_type()
df_schoolTypeDim = load_dimension_data_edex.get_dim_school_type_codes()

df = pd.merge(left = df_memberToSchoolTypeDim, right = df_schoolTypeDim, how = "inner", left_on = "schoolTypeID", right_on = "id")

df_1 = df.groupby(["memberId"]).size().reset_index(name="counts_memberID")
df_2 = df.groupby(["memberId","schoolCategory"]).size().reset_index(name="counts_memberID_schoolCategory")

df_3 = pd.merge(left = df_1, right = df_2, how = "inner", on = "memberId")
df_3["eduRate"] = df_3["counts_memberID_schoolCategory"]/df_3["counts_memberID"]

df_4 = df_3[df_3["eduRate"]==1]
df_4["class_tmp"] = df_4["schoolCategory"]
df_4["class"] = df_4["schoolCategory"]
df_4["rule"] = "4"
df_4["createdAt"] = date.today()
df_4 = df_4[(df_4["class"] == "K12") | (df_4["class"] == "HED")]
df_segExperienceSchoolType_1 = df_4[["memberId", "class_tmp", "class", "rule", "createdAt"]]

df_4 = df_3[(df_3["eduRate"]>0.5) & (df_3["eduRate"] < 1)]
df_4["class_tmp"] = "Possible"
df_4["class"] = df_4["schoolCategory"]
df_4["rule"] = "4"
df_4["createdAt"] = date.today()
df_4 = df_4[(df_4["class"] == "K12") | (df_4["class"] == "HED")]
df_segExperienceSchoolType_2 = df_4[["memberId", "class_tmp", "class", "rule", "createdAt"]]

df_rule4 = pd.concat([df_segExperienceSchoolType_1, df_segExperienceSchoolType_2], join="outer", ignore_index=True)
df_rule4 = df_rule4.drop_duplicates()

del df, df_1, df_2, df_3, df_4, df_segExperienceSchoolType_1, df_segExperienceSchoolType_2
print("Rule 4 finished...", datetime.now())
# //RULE 4


# RULE 5
print("Rule 5 started...", datetime.now())
rule5_query = """
select distinct t.memberGuid as memberId, 
       '5.1' as class_tmp, 
       case when lower(trim(t.target_group)) = 'edgen - ad/ed/fac/staff' then 'K12'
            when lower(trim(t.target_group)) = 'education - general' then 'K12'
            when lower(trim(t.target_group)) = 'k-12 - admin' then 'K12'
            when lower(trim(t.target_group)) = 'k-12 - ed/fac/staff' then 'K12'
            when lower(trim(t.target_group)) = 'hed - admin' then 'HED'
            when lower(trim(t.target_group)) = 'hed - ed/fac/staff' then 'HED'
            when lower(trim(t.target_group)) = 'hed - student' then 'HED'
            else 'other'
        end as class,
        "5" as rule, 
        CURDATE() as createdAt
from hdp.EdexMemberSegmentationExtract t
join edx.Member m on m.id = t.memberGuid
where t.target_group in ('K-12 - Admin', 'K-12 - Ed/Fac/Staff', 'Education - General', 'EdGen - Ad/Ed/Fac/Staff', 'HED - Admin', 'HED - Ed/Fac/Staff', 'HED - Student')
union all
select distinct e.memberId, '5.2' as class_tmp, t.class_ as class, "5" as rule, CURDATE() as createdAt
from hdp.enterprise_dim_org_education t
join hdp.EntMemberLicenseDelegation d on d.org_id = t.org_id
join hdp.MemberGuidMap e on e.userGuid = d.member_guid
and t.class_ in ('K12', 'HED');
"""
print("Rule 5 finished...", datetime.now())
#
# // RULE 5


# RULE 6
# '02dd452b-7f08-4774-aae4-cfcb1336acf4' -- K12 Design Your Creative Class
# '8e7968d8-914e-4ac1-bc45-23358fec9dfb' -- HED Design Your Creative Course
print("Rule 6 started...", datetime.now())
rule6_query = """
select e.memberID, 'K12' as class_tmp, 'K12' as class, "6" as rule, CURDATE() as createdAt
from edx.Enrollment e
where e.courseID = '02dd452b-7f08-4774-aae4-cfcb1336acf4'
and e.progress = 100
and e.memberID not in (
	select e1.memberId
	from edx.Enrollment e1
	where e1.courseID = '8e7968d8-914e-4ac1-bc45-23358fec9dfb'
	and e1.progress = 100)
union all
select e.memberID, 'HED' as class_tmp, 'HED' as class, "6" as rule, CURDATE() as createdAt
from edx.Enrollment e
where e.courseID = '8e7968d8-914e-4ac1-bc45-23358fec9dfb'
and e.progress = 100
and e.memberID not in (
	select e1.memberId
	from edx.Enrollment e1
	where e1.courseID = '02dd452b-7f08-4774-aae4-cfcb1336acf4'
	and e1.progress = 100);
"""
print("Rule 6 finished...", datetime.now())
# // RULE 6


# RULE 7
print("Rule 7 started...", datetime.now())
df_courseToAcademicLevel = load_dimension_data_edex.get_dim_course_to_academic_level()
df_academicLevel = load_dimension_data_edex.get_dim_academic_level_codes()
df_enrollmentDim = load_dimension_data_edex.get_dim_enrollment()

df = pd.merge(left = df_courseToAcademicLevel, right = df_academicLevel, how = "inner", left_on = "academicLevels", right_on = "id")

df_1 = df.groupby(["courseId"]).size().reset_index(name="counts_courseId")
df_2 = df.groupby(["courseId","eduLevel"]).size().reset_index(name="counts_courseId_eduLevel") #tmpCourseToAcademicLevelEduNum

df_3 = pd.merge(left = df_1, right = df_2, how = "inner", on = "courseId")
df_3["eduRate"] = df_3["counts_courseId_eduLevel"]/df_3["counts_courseId"] #tmpCourseToAcademicLevelEduFinal

df_4 = pd.merge(left = df_enrollmentDim, right = df_3, how = "inner", left_on = "courseID", right_on = "courseId")
df_4 = df_4[(df_4["progress"]==100)]

df_5 = df_4[df_4["eduRate"]==1]
df_5["class_tmp"] = df_5["eduLevel"]
df_5["class"] = df_4["eduLevel"]
df_5["rule"] = "7"
df_5["createdAt"] = date.today()
df_5 = df_5[(df_5["class"] == "K12") | (df_5["class"] == "HED")]
df_segMemberCourseEnrollment_1 = df_5[["memberID", "class_tmp", "class", "rule", "createdAt"]]


df_5 = df_4[(df_4["eduRate"]>0.5) & (df_4["eduRate"] < 1)]
df_5["class_tmp"] = "Possible"
df_5["class"] = df_5["eduLevel"]
df_5["rule"] = "7"
df_5["createdAt"] = date.today()
df_5 = df_5[(df_5["class"] == "K12") | (df_5["class"] == "HED")]
df_segMemberCourseEnrollment_2 = df_5[["memberID", "class_tmp", "class", "rule", "createdAt"]]

df_segMemberCourseEnrollment_1 = df_segMemberCourseEnrollment_1.drop_duplicates()
df_segMemberCourseEnrollment_2 = df_segMemberCourseEnrollment_2.drop_duplicates()
del df, df_1, df_2, df_3, df_4, df_5

# Additional rule for rule 7 - rule 9 prev called
# select courses tagged as K12 and HED only
df_courseToAcademicLevelK12 = pd.merge(left = df_courseToAcademicLevel, right = df_academicLevel, how = "inner", left_on = "academicLevels", right_on = "id")
df_courseToAcademicLevelK12 = df_courseToAcademicLevelK12[(df_courseToAcademicLevelK12["eduLevel"]=="K12")]
df_courseToAcademicLevelK12 = df_courseToAcademicLevelK12[["courseId","eduLevel"]]
df_courseToAcademicLevelK12 = df_courseToAcademicLevelK12.drop_duplicates()

df_courseToAcademicLevelHED = pd.merge(left = df_courseToAcademicLevel, right = df_academicLevel, how = "inner", left_on = "academicLevels", right_on = "id")
df_courseToAcademicLevelHED = df_courseToAcademicLevelHED[(df_courseToAcademicLevelHED["eduLevel"]=="HED")]
df_courseToAcademicLevelHED = df_courseToAcademicLevelHED[["courseId","eduLevel"]]
df_courseToAcademicLevelHED = df_courseToAcademicLevelHED.drop_duplicates()

df_coursesK12HED = pd.merge(df_courseToAcademicLevelK12, df_courseToAcademicLevelHED, how="left", on = ["courseId"], indicator=True)
df_coursesK12 = df_coursesK12HED[(df_coursesK12HED["_merge"]=="left_only")]
df_coursesK12 = df_coursesK12.drop_duplicates()

df_coursesHED = df_coursesK12HED[(df_coursesK12HED["_merge"]=="right_only")]
df_coursesHED = df_coursesHED.drop_duplicates()

# 1
df_enrollemntCourse = pd.merge(left = df_enrollmentDim, right = df_coursesK12, how = "inner", left_on = "courseID", right_on = "courseId")
df_enrollemntCourse = df_enrollemntCourse[["memberID", "courseID"]]
df_enrollemntCourse = df_enrollemntCourse.drop_duplicates()

# 2
df_enrollemntK12Course = df_enrollmentDim[~df_enrollmentDim.courseID.isin(df_coursesK12.courseId)] #select e2.memberId from ra.enrollmentedexdim e2 where e2.courseId not in (select t.courseId from ra.onlyK12HEDCourse  t where t.eduLevel = 'K12')
df_enrollemntK12Course = df_enrollemntK12Course[["memberID"]]
df_enrollemntK12Course = df_enrollemntK12Course.drop_duplicates()

# 3
df_rule7A = df_enrollemntCourse[~df_enrollemntCourse.memberID.isin(df_enrollemntK12Course.memberID)] #e.memberId not in (select e2.memberId..
df_rule7A = df_rule7A[["memberID"]]
df_rule7A = df_rule7A.drop_duplicates()

# No HED only
df_rule7A["class_tmp"] = "K12"
df_rule7A["class"] = "K12"
df_rule7A["rule"] = "2"
df_rule7A["createdAt"] = date.today()


df_rule7 = pd.concat([df_segMemberCourseEnrollment_1, df_segMemberCourseEnrollment_2, df_rule7A], join="outer", ignore_index=True)
df_rule7 = df_rule7.drop_duplicates()

del df_segMemberCourseEnrollment_1, df_segMemberCourseEnrollment_2, df_rule7A
del df_enrollemntCourse, df_enrollemntK12Course, df_courseToAcademicLevelK12, df_courseToAcademicLevelHED, df_courseToAcademicLevel, df_enrollmentDim
del df_coursesHED, df_coursesK12HED,df_academicLevel
# df_coursesK12 is used in Rule 8
print("Rule 7 finished...", datetime.now())
# //RULE 7


# RULE 9
print("Rule 9 started...", datetime.now())
rule9_query = """
select distinct e.memberId, 'K12' as class_tmp, 'K12' as class, 9 as rule, CURDATE() as createdAt
from edx.Enrollment e
where courseId in (
select c.id
from edx.Course c
where c.academicLevels not like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%')
and e.memberId not in (
select e1.memberId 
from edx.Enrollment e1 
join edx.Course c1 on c1.id = e1.courseId
where c1.academicLevels like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%')
and e.progress >= 10
union all
-- K12
select distinct e.memberId, 'Possible K12' as class_tmp, 'K12' as class, 9 as rule, CURDATE() as createdAt
from edx.Enrollment e
where courseId in (
select c.id
from edx.Course c
where c.academicLevels not like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%')
and e.memberId not in (
select e1.memberId 
from edx.Enrollment e1 
join edx.Course c1 on c1.id = e1.courseId
where c1.academicLevels like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%')
and e.progress < 10
union all
-- HED
select distinct e.memberId, 'HED' as class_tmp, 'HED' as class, 9 as rule, CURDATE() as createdAt
from edx.Enrollment e
where e.courseId = '8a2ae22b-535c-4a67-8364-9c86d80d3c67'
and e.memberId not in (select e1.memberId from edx.Enrollment e1 where e1.courseId != '8a2ae22b-535c-4a67-8364-9c86d80d3c67')
union
-- HED
select distinct e.memberId, 'Possible HED' as class_tmp, 'HED' as class, 9 as rule, CURDATE() as createdAt
from edx.Enrollment e
where e.courseId = '8a2ae22b-535c-4a67-8364-9c86d80d3c67'
and e.memberId not in (
select e1.memberId 
from edx.Enrollment e1 
join edx.Course c1 on c1.id = e1.courseId
where c1.academicLevels not like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%');
"""
print("Rule 9 finished...", datetime.now())
#
# // RULE 9


# RULE 10
# Additional rule (Based on Rule 2 - Only Primary AcademicLevels from Resource)
print("Rule 10 started...", datetime.now())
df_memberEduLevel = get_member_edu_level()

df_memberEduLevel = df_memberEduLevel[(df_memberEduLevel["layer"] == "primary")]
df_memberEduLevel = df_memberEduLevel.drop(columns=["layer"])

df_memberEduLevelK12 = df_memberEduLevel[(df_memberEduLevel["eduLevel"] == "K12")]
df_memberEduLevelHED = df_memberEduLevel[(df_memberEduLevel["eduLevel"] == "HED")]

# samo memberi koji imaju Created\Viewed\Downloaded K12 ili HED resurse
df_resourceK12Only = df_memberEduLevelK12[~df_memberEduLevel.memberId.isin(df_memberEduLevelHED.memberId)]
df_resourceHEDOnly = df_memberEduLevelHED[~df_memberEduLevel.memberId.isin(df_memberEduLevelK12.memberId)]

# possible members
df_possibleK12HED = pd.merge(left = df_memberEduLevelK12, right = df_memberEduLevelHED, how = "inner", left_on = "memberId", right_on = "memberId")
df_possibleK12HED["sum_EventCountRateK12"] = df_possibleK12HED["sum_EventCount_x"]/df_possibleK12HED["sum_EventCount_y"]
df_possibleK12HED["sum_EventCountRateHED"] = df_possibleK12HED["sum_EventCount_y"]/df_possibleK12HED["sum_EventCount_x"]
#df_possibleK12HED["sum_EventCountRateK12"] = df_possibleK12HED["cnt_memberEduLevel_x"]/df_possibleK12HED["cnt_memberEduLevel_y"]
#df_possibleK12HED["sum_EventCountRateHED"] = df_possibleK12HED["cnt_memberEduLevel_y"]/df_possibleK12HED["cnt_memberEduLevel_x"]

df_possibleK12 = df_possibleK12HED[(df_possibleK12HED["sum_EventCountRateK12"]>1.5)]
df_possibleHED = df_possibleK12HED[(df_possibleK12HED["sum_EventCountRateHED"]>1.5)]

df_possibleK12 = df_possibleK12[["memberId", "eduLevel_x", "eduLevel_x"]]
df_possibleK12.columns = ["memberId", "class_tmp", "class"]
df_possibleK12["class_tmp"] = "Possible K12"
df_possibleK12["rule"] = "10"
df_possibleK12["createdAt"] = date.today()

df_possibleHED = df_possibleHED[["memberId", "eduLevel_y", "eduLevel_y"]]
df_possibleHED.columns = ["memberId", "class_tmp", "class"]
df_possibleHED["class_tmp"] = "Possible HED"
df_possibleHED["rule"] = "10"
df_possibleHED["createdAt"] = date.today()

df_resourceK12Only = df_resourceK12Only[["memberId", "eduLevel", "eduLevel"]]
df_resourceK12Only.columns = ["memberId", "class_tmp", "class"]
df_resourceK12Only["rule"] = "10"
df_resourceK12Only["createdAt"] = date.today()

df_resourceHEDOnly = df_resourceHEDOnly[["memberId", "eduLevel", "eduLevel"]]
df_resourceHEDOnly.columns = ["memberId", "class_tmp", "class"]
df_resourceHEDOnly["rule"] = "10"
df_resourceHEDOnly["createdAt"] = date.today()

del df_memberEduLevel, df_memberEduLevelK12, df_memberEduLevelHED, df_possibleK12HED

# Additional, based on Resource and ResourceToAcademicLevel
df_resourceDim = load_dimension_data_edex.get_dim_resource()
df_resourceToAcademicLevel = load_dimension_data_edex.get_dim_resource_to_academic_level()
df_academicLevelDim = load_dimension_data_edex.get_dim_academic_level_codes()

df_resurceAcademicLevel = pd.merge(left = df_resourceToAcademicLevel, right = df_resourceDim, how = "inner", left_on = "resourceId", right_on = "id")
df_resurceAcademicLevel = pd.merge(left = df_resurceAcademicLevel, right = df_academicLevelDim, how = "inner", left_on = "academicLevels_x", right_on = "id")

df_resurceAcademicLevel = df_resurceAcademicLevel[(df_resurceAcademicLevel["layer"] == "primary")]

df_resurceAcademicLevel = df_resurceAcademicLevel[["createdBy", "eduLevel"]]
df_resurceAcademicLevel = df_resurceAcademicLevel.groupby(["createdBy", "eduLevel"]).size().reset_index(name="cnt_")
df_resurceAcademicLevel.columns = ["memberId", "eduLevel", "cnt_"]

df_resurceAcademicLevel = df_resurceAcademicLevel[(df_resurceAcademicLevel["eduLevel"] == "K12") | (df_resurceAcademicLevel["eduLevel"] == "HED")]

df_resurceAcademicLevelK12 = df_resurceAcademicLevel[(df_resurceAcademicLevel["eduLevel"] == "K12")]
df_resurceAcademicLevelHED = df_resurceAcademicLevel[(df_resurceAcademicLevel["eduLevel"] == "HED")]


# K12 only
df_resurceAcademicLevelK12Only = df_resurceAcademicLevel[~df_resurceAcademicLevel.memberId.isin(df_resurceAcademicLevelHED.memberId)]
df_resurceAcademicLevelK12Only = df_resurceAcademicLevelK12Only[["memberId", "eduLevel", "eduLevel"]]
df_resurceAcademicLevelK12Only.columns = ["memberId", "class_tmp", "class"]
df_resurceAcademicLevelK12Only["rule"] = "10"
df_resurceAcademicLevelK12Only["createdAt"] = date.today()

# HED only
df_resurceAcademicLevelHEDOnly = df_resurceAcademicLevel[~df_resurceAcademicLevel.memberId.isin(df_resurceAcademicLevelK12.memberId)]
df_resurceAcademicLevelHEDOnly = df_resurceAcademicLevelHEDOnly[["memberId", "eduLevel", "eduLevel"]]
df_resurceAcademicLevelHEDOnly.columns = ["memberId", "class_tmp", "class"]
df_resurceAcademicLevelHEDOnly["rule"] = "10"
df_resurceAcademicLevelHEDOnly["createdAt"] = date.today()

df_resurceAcademicLevelPossible =  pd.merge(left = df_resurceAcademicLevel, right = df_resurceAcademicLevel, how = "inner", left_on = "memberId", right_on = "memberId")
# more K12 than HED
df_resurceAcademicLevelPossibleK12 = df_resurceAcademicLevelPossible[(df_resurceAcademicLevelPossible["eduLevel_x"] == "K12") & (df_resurceAcademicLevelPossible["eduLevel_y"] == "HED") & (df_resurceAcademicLevelPossible["cnt__x"] > df_resurceAcademicLevelPossible["cnt__y"])]
df_resurceAcademicLevelPossibleK12 = df_resurceAcademicLevelPossibleK12[["memberId", "eduLevel_x", "eduLevel_x"]]
df_resurceAcademicLevelPossibleK12.columns = ["memberId", "class_tmp", "class"]
df_resurceAcademicLevelPossibleK12["class_tmp"]= "Possible K12"
df_resurceAcademicLevelPossibleK12["rule"] = "10"
df_resurceAcademicLevelPossibleK12["createdAt"] = date.today()

# more HED than K12
df_resurceAcademicLevelPossibleHED = df_resurceAcademicLevelPossible[(df_resurceAcademicLevelPossible["eduLevel_x"] == "HED") & (df_resurceAcademicLevelPossible["eduLevel_y"] == "K12") & (df_resurceAcademicLevelPossible["cnt__x"] > df_resurceAcademicLevelPossible["cnt__y"])]
df_resurceAcademicLevelPossibleHED = df_resurceAcademicLevelPossibleHED[["memberId", "eduLevel_x", "eduLevel_x"]]
df_resurceAcademicLevelPossibleHED.columns = ["memberId", "class_tmp", "class"]
df_resurceAcademicLevelPossibleHED["class_tmp"]= "Possible HED"
df_resurceAcademicLevelPossibleHED["rule"] = "10"
df_resurceAcademicLevelPossibleHED["createdAt"] = date.today()


df_rule10 = pd.concat([df_possibleK12, df_possibleHED, df_resourceK12Only, df_resourceHEDOnly, df_resurceAcademicLevelK12Only, df_resurceAcademicLevelHEDOnly, df_resurceAcademicLevelPossibleK12, df_resurceAcademicLevelPossibleHED], join="outer", ignore_index=True)
df_rule10 = df_rule10.drop_duplicates()

del df_resourceDim, df_resourceToAcademicLevel, df_academicLevelDim, df_resurceAcademicLevel, df_resurceAcademicLevelPossible, df_resurceAcademicLevelK12, df_resurceAcademicLevelHED
del df_possibleK12, df_possibleHED, df_resourceK12Only, df_resourceHEDOnly, df_resurceAcademicLevelK12Only, df_resurceAcademicLevelHEDOnly, df_resurceAcademicLevelPossibleK12, df_resurceAcademicLevelPossibleHED

print("Rule 10 finished...", datetime.now())
# // RULE 10

dbConnection = pymysql.connect(
    host=local_host_, user=local_user_,
    password=local_pass_, port=local_port_
)
try:
    with dbConnection.cursor() as cursor:
        # create history table
        table = "edx.MemberSegmentation"
        backupTable = "ra.MemberSegmentation_" + date.today().strftime('%Y%m%d')
        data_utils.insert_to_backup_table(dbConnection, table, backupTable, dropBackupTable_ = 1)
        # create prev table, same content
        # prevTable = "edx.MemberSegmentation_prev"
        # data_utils.insert_to_backup_table(dbConnection, table, prevTable, dropBackupTable_ = 1)
        dbConnection.commit()

        # operational segmenatation table
        query = "delete from edx.MemberSegmentation_temp;"
        cursor.execute(query)
        dbConnection.commit()

        # RULE 1
        print("Rule 1 to db started ...", datetime.now())
        cursor.execute(emailMembers_query)
        rows = cursor.fetchall()
        df_emailMembers = pd.DataFrame(rows, columns = ["memberId", "class_tmp", "class", "rule", "createdAt"], index = None)
        df_emailMembers = df_emailMembers.drop_duplicates()
        data_utils.insert_df_data_to_table(df_emailMembers, dbConnection, tableMemberSegmentationTemp)
        if not df_emailMembers.empty:
            dbConnection.commit()
        del rows
        print("Rule 1 to db finished... ", datetime.now())

        # RULE 11
        print("Rule 11 to db started ...", datetime.now())
        cursor.execute(emailDomain_query)
        rows = cursor.fetchall()
        df_emailDomain = pd.DataFrame(rows, columns = ["memberId", "class_tmp", "class", "rule", "createdAt"], index = None)
        df_emailDomain = df_emailDomain.drop_duplicates()
        data_utils.insert_df_data_to_table(df_emailDomain, dbConnection, tableMemberSegmentationTemp)
        if not df_emailDomain.empty:
            dbConnection.commit()
        del rows
        print("Rule 11 to db finished... ", datetime.now())
        

        # RULE 2
        print("Rule 2 to db started ...", datetime.now())
        df_rule2 = df_rule2.drop_duplicates()
        data_utils.insert_df_data_to_table(df_rule2, dbConnection, tableMemberSegmentationTemp)

        if not df_rule2.empty:
            dbConnection.commit()
        print("Rule 2 to db finished... ", datetime.now())

        # RULE 3
        print("Rule 3 to db started ...", datetime.now())
        df_rule3 = df_rule3.drop_duplicates()
        data_utils.insert_df_data_to_table(df_rule3, dbConnection, tableMemberSegmentationTemp)
        if not df_rule3.empty:
            dbConnection.commit()
        print("Rule 3 to db finished... ", datetime.now())

        # RULE 4
        print("Rule 4 to db started ...", datetime.now())
        df_rule4 = df_rule4.drop_duplicates()        
        data_utils.insert_df_data_to_table(df_rule4, dbConnection, tableMemberSegmentationTemp) # Rate > 0.5
        if not df_rule4.empty:
            dbConnection.commit()
        print("Rule 4 to db finished... ", datetime.now())
        
        # RULE 5
        print("Rule 5 to db started ...", datetime.now())
        cursor.execute(rule5_query)
        rows = cursor.fetchall()
        df_orgIdRule5 = pd.DataFrame(rows, columns = ["memberId", "class_tmp", "class", "rule", "createdAt"], index = None)
        df_orgIdRule5 = df_orgIdRule5.drop_duplicates()
        data_utils.insert_df_data_to_table(df_orgIdRule5, dbConnection, tableMemberSegmentationTemp)
        if not df_orgIdRule5.empty:
            dbConnection.commit()
        del rows
        print("Rule 5 to db finished ...", datetime.now())
        
        # RULE 6
        print("Rule 6 to db started ...", datetime.now())
        cursor.execute(rule6_query)
        rows = cursor.fetchall()
        df_orgIdRule6 = pd.DataFrame(rows, columns = ["memberId", "class_tmp", "class", "rule", "createdAt"], index = None)
        df_orgIdRule6 = df_orgIdRule6.drop_duplicates()
        data_utils.insert_df_data_to_table(df_orgIdRule6, dbConnection, tableMemberSegmentationTemp)
        if not df_orgIdRule6.empty:
            dbConnection.commit()
        del rows
        print("Rule 6 to db finished ...", datetime.now())

        # RULE 7 (+ exRule 9)
        print("Rule 7 to db started ...", datetime.now())
        df_rule7 = df_rule7.drop_duplicates()
        data_utils.insert_df_data_to_table(df_rule7, dbConnection, tableMemberSegmentationTemp)
        if not df_rule7.empty:
            dbConnection.commit()
        print("Rule 7 to db finished ...", datetime.now())
        
        # RULE 9
        print("Rule 9 to db started ...", datetime.now())
        cursor.execute(rule9_query)
        rows = cursor.fetchall()
        df_Rule9 = pd.DataFrame(rows, columns = ["memberId", "class_tmp", "class", "rule", "createdAt"], index = None)
        df_Rule9 = df_Rule9.drop_duplicates()
        data_utils.insert_df_data_to_table(df_Rule9, dbConnection, tableMemberSegmentationTemp)
        if not df_Rule9.empty:
            dbConnection.commit()
        del rows
        print("Rule 9 to db finished ...", datetime.now())
        
        # RULE 10
        print("Rule 10 to db started ...", datetime.now())
        df_rule10 = df_rule10.drop_duplicates()
        data_utils.insert_df_data_to_table(df_rule10, dbConnection, tableMemberSegmentationTemp)

        if not df_rule10.empty:
            dbConnection.commit()
        print("Rule 2 to db finished... ", datetime.now())
finally:
    dbConnection.close()


# Needs to be last because memberSegmentationTemp
# RULE 8
print("Rule 8 started ...", datetime.now())
#reload(LoadDimensionDataEdEx)


# import local table .. promeniti na edx.Membersegmentation
df_memberSegmentationTemp = load_dimension_data_table.get_table_data("edx.memberSegmentation_temp") #25.11.2022
#df_memberSegmentationTemp = LoadDimensionDataEdEx.getDimMemberSegmentationTemp() #25.11.2022

df_enrollmentDim = load_dimension_data_edex.get_dim_enrollment()
df_courseAcademicLevelDim = load_dimension_data_edex.get_dim_course_to_academic_level()
df_academicLevelDim = load_dimension_data_edex.get_dim_academic_level_codes()

## temp
#df_memberSegmentationTemp = df_memberSegmentationTemp[(df_memberSegmentationTemp["rule"] != "8")]
df_memberSegmentationTemp = df_memberSegmentationTemp[~df_memberSegmentationTemp["rule"].isin(["8","10"])]

# 1 all members by course
df_membersByCourse = df_enrollmentDim.groupby(["courseID"]).size().reset_index(name="cnt_membersByCourse")
## df_membersByCourse = df_membersByCourse[(df_membersByCourse["courseID"] == "02d6f338-9356-41f1-8feb-d434828eba96")]

# 2 classified members by course
df_classifiedMembersByCourse = pd.merge(left = df_enrollmentDim, right = df_memberSegmentationTemp, how = "inner", left_on = "memberID", right_on = "memberId")

#df_classifiedMembersByCourse = df_classifiedMembersByCourse.groupby(["courseID"]).size().reset_index(name="cnt_classifiedMembersByCourse")
df_classifiedMembersByCourse = df_classifiedMembersByCourse.groupby(["courseID"])["memberID"].nunique().reset_index(name="cnt_classifiedMembersByCourse")

# 3 courses candidates for rule 8
df_courseCandidates = pd.merge(left = df_membersByCourse, right = df_classifiedMembersByCourse, how = "inner", left_on = "courseID", right_on = "courseID")
df_courseCandidates["courseCandidatesRate"] = df_courseCandidates["cnt_classifiedMembersByCourse"]/df_courseCandidates["cnt_membersByCourse"]
df_courseCandidates = df_courseCandidates[(df_courseCandidates["courseCandidatesRate"]>0.5)]

# 4 members by class within course
df_courseByClass = pd.merge(left = df_enrollmentDim, right = df_memberSegmentationTemp, how = "inner", left_on = "memberID", right_on = "memberId")

#df_courseByClass = df_courseByClass.groupby(["courseID", "class"]).size().reset_index(name="cnt_courseByClass")
df_courseByClass = df_courseByClass.groupby(["courseID", "class"])["memberID"].nunique().reset_index(name="cnt_courseByClass")

# 5 rate between all members by course and class members by course
df_courseByClassRate_1 = pd.merge(left = df_membersByCourse, right = df_courseByClass, how = "inner", left_on = "courseID", right_on = "courseID")

#df_courseByClassRate_1["courseByClassRateRate"] = df_courseByClassRate_1["cnt_courseByClass"]/df_courseByClassRate_1["cnt_membersByCourse"]

df_courseByClassRate_2  = pd.merge(left = df_courseByClassRate_1, right = df_courseCandidates, how = "inner", left_on = "courseID", right_on = "courseID")
df_courseByClassRate_2["courseByClassRateRate"] = df_courseByClassRate_2["cnt_courseByClass"]/df_courseByClassRate_2["cnt_classifiedMembersByCourse"]

df_courseByClassRate = df_courseByClassRate_2[(df_courseByClassRate_2["courseByClassRateRate"]>0.5)]

# 6
df_courseByClassRate = df_courseByClassRate[["courseID", "class"]]
df_courseByClassRate = df_courseByClassRate.drop_duplicates()
df_rule8_1 = pd.merge(left = df_enrollmentDim, right = df_courseByClassRate, how = "inner", left_on = "courseID", right_on = "courseID")
df_rule8_1 = df_rule8_1[["memberID","courseID","class"]]


# deeds df_coursesK12 from Rule 7
df_rule8 = df_rule8_1[(~df_rule8_1.courseID.isin(df_coursesK12.courseId)) & (~df_rule8_1.memberID.isin(df_memberSegmentationTemp.memberId))]

df_rule8 = df_rule8[["memberID", "class", "class"]]
df_rule8 = df_rule8.drop_duplicates()
df_rule8.columns = ["memberId", "class_tmp", "class"]
df_rule8["rule"] = "8"
df_rule8["createdAt"] = date.today()

del df_membersByCourse, df_classifiedMembersByCourse, df_courseCandidates, df_courseByClassRate_1, df_courseByClassRate_2, df_courseByClassRate, df_rule8_1
del df_enrollmentDim, df_academicLevelDim, df_courseAcademicLevelDim, df_courseByClass, df_memberSegmentationTemp
#del df_coursesK12


dbConnection = pymysql.connect(
    host=local_host_, user=local_user_,
    password=local_pass_, port=local_port_
)
try:
    with dbConnection.cursor() as cursor:
        # RULE 8
        print("Rule 8 to db started ...", datetime.now())
        df_rule8 = df_rule8.drop_duplicates()
        data_utils.insert_df_data_to_table(df_rule8, dbConnection, tableMemberSegmentationTemp)
        if not df_rule8.empty:
            dbConnection.commit()
        print("Rule 8 to db finished ...", datetime.now())
finally:
    dbConnection.close()
# //RULE 8

print("------------------------------------------------------------------------------------")
# Run process member segmentation sql file
##
import core.utils.run_sql_file_local as run_sql_file_local

open_sql_file_scripts = open("import\\params\\sql_file_scripts.json", "r")
load_sql_file = json.load(open_sql_file_scripts)

sql_file_path = load_sql_file.get("process_member_segmentation_table_sql_file")

run_sql_file_local.execute_sql_file_on_local_database(sql_file_path)

print("------------------------------------------------------------------------------------")
# Export edx.MemberSegmentation to .gz file
##
path_ = conn_params.get("member_segementation_file_export_path")
backupFileName_ = "membersegmentation_" + date.today().strftime('%Y%m%d')
fileName_ = "membersegmentation"
fileExtension_ = ""
fileSeparator_ = ","

df_export = load_dimension_data_table.get_table_data("edx.membersegmentation")

# create a backup file
data_utils.export_data_to_file(df_export, path_, backupFileName_, fileExtension_, fileSeparator_, archive_="gzip", fileHeader_=False)

# export to file
data_utils.export_data_to_file(df_export, path_, fileName_, fileExtension_, fileSeparator_, archive_="gzip", fileHeader_=False)

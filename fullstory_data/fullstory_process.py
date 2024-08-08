import sys
sys.path.append("import") # because load_dimension_data_table is in another directory
import core.utils.insert_data_to_local_database as insert_data_to_local_database
import core.utils.run_sql_query_local as run_sql_query_local
import pandas as pd
from datetime import datetime
import re


# Resources
print("Start Resources...", datetime.now())

query_resource = """
select r.id,
       r.SEOUrl,
       l.layer,
       a.urlLabel,
       a.eduLevel
from edx.Resource r 
join edx.ResourceToAcademicLevel l on l.resourceId = r.id 
left join edx.AcademicLevel a on a.id = l.academicLevels
where r.SEOUrl is not null 
and r.SEOUrl != '';
"""

query_visitor_tr_pages = """
select  t.IndvId, 
        t.PageUrl, 
        t.fiscal_yr_and_per
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.PageUrl like '%teaching-resources%';
"""


df_resource = run_sql_query_local.get_data_from_sql_query_local_database(query_resource)
df_resource = df_resource.drop_duplicates()
df_resource['SEOUrl'] = df_resource['SEOUrl'].fillna('')

df_visitor_pages = run_sql_query_local.get_data_from_sql_query_local_database(query_visitor_tr_pages)
df_visitor_pages = df_visitor_pages.drop_duplicates()
df_visitor_pages['PageUrl'] = df_visitor_pages['PageUrl'].fillna('')

df_page_teaching_resources = pd.DataFrame(columns=['IndvId', 'PageUrl', 'fiscal_yr_and_per', 'PageSeoUrl'])
for index_1, row_1 in df_visitor_pages.iterrows():
    # search substring afer "teaching-resources/"
    result = re.search(re.escape("teaching-resources/") + r"(.*?)$", row_1['PageUrl'])
    
    if result:
        row_series = pd.Series({"IndvId": row_1['IndvId'], "PageUrl": row_1['PageUrl'], "fiscal_yr_and_per": row_1['fiscal_yr_and_per'], "PageSeoUrl": result.group(1)})
        df_page_teaching_resources.loc[len(df_page_teaching_resources)] = row_series


df_visitor_resources = pd.merge(df_page_teaching_resources, df_resource, left_on='PageSeoUrl', right_on='SEOUrl', how = 'inner')
#df_primary = df_visitor_resources[df_visitor_resources["layer"]=="primary"]

if not df_visitor_resources.empty:
    schema = "ra"
    table = "fullstory_tmp_visitor_resources"
    insert_data_to_local_database.insert_data_to_table(schema, table, df_visitor_resources, "replace")
    
print("End Resources...", datetime.now())


# Courses
print("Start Courses...", datetime.now())

query_course = """
select c.id,
       c.vanityURL,
       -- c.title,
       -- c.shortDescription,
       -- c.courseTypeID,
       c.academicLevels as caourseAcademicLevels,
       l.academicLevels,      
       a.urlLabel,
       a.eduLevel
from edx.course c
join edx.CourseToAcademicLevel l on l.courseId = c.id
left join edx.AcademicLevel a on a.id = l.academicLevels;
"""

query_visitor_pl_pages = """
select  t.IndvId, 
        t.PageUrl,
        t.fiscal_yr_and_per
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.PageUrl like '%professional-learning/%';
"""

df_course = run_sql_query_local.get_data_from_sql_query_local_database(query_course)
df_course = df_course.drop_duplicates()

df_visitor_pages = run_sql_query_local.get_data_from_sql_query_local_database(query_visitor_pl_pages)
df_visitor_pages = df_visitor_pages.drop_duplicates()
df_visitor_pages['PageUrl'] = df_visitor_pages['PageUrl'].fillna('')


df_page_professional_learning = pd.DataFrame(columns=['IndvId', 'PageUrl', 'fiscal_yr_and_per', 'PageCourseUrl'])
for index_1, row_1 in df_visitor_pages.iterrows():
    result = re.search(r'self-paced-course/(.*?)/', row_1['PageUrl'])
    
    if result:
        row_series = pd.Series({"IndvId": row_1['IndvId'], "PageUrl": row_1['PageUrl'], "fiscal_yr_and_per": row_1['fiscal_yr_and_per'], "PageCourseUrl": result.group(1)})
        df_page_professional_learning.loc[len(df_page_professional_learning)] = row_series


df_visitor_courses = pd.merge(df_page_professional_learning, df_course, left_on='PageCourseUrl', right_on='vanityURL', how = 'inner')

if not df_visitor_courses.empty:
    schema = "ra"
    table = "fullstory_tmp_visitor_courses"
    insert_data_to_local_database.insert_data_to_table(schema, table, df_visitor_courses, "replace") 
print("End Courses...", datetime.now())

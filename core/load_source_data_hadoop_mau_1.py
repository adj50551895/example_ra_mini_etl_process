import sys
sys.path.append("import") # because load_dimension_data_table_v1 is in another directory
import core.utils.load_data_from_hadoop as load_data_from_hadoop
import core.utils.insert_data_to_local_database as insert_data_to_local_database
import core.utils.run_sql_query_local as run_sql_query_local
from datetime import date, timedelta


# ## current_fiscal_yr_and_per = run_sql_query_local.get_data_from_sql_query_local_database("select fiscal_yr_and_per from edx.hana_dim_date where calendar_date = current_date();")
# ## current_fiscal_yr_and_per = current_fiscal_yr_and_per.iloc[0,0]
# ## current_fiscal_yr_and_per = str(current_fiscal_yr_and_per)

# 1 content_detail
# user_gk.edex_spark_usage
# hdp.spark_event_activity_b_logins
max_event_date = run_sql_query_local.get_data_from_sql_query_local_database("select max(date(max_event_date)) from hdp.spark_event_activity_b_logins;")
date_from = max_event_date.iloc[0,0]
date_from = date_from + timedelta(days=1)
date_to = date.today() - timedelta(days=1) # two days ago
date_from = date_from.strftime('%Y-%m-%d')
date_to = date_to.strftime('%Y-%m-%d')

query_spark_event_activity_logins = """
select c.user_guid,
       mp.original_guid,
       dd.fiscal_yr_and_per,
       min(c.event_date) as min_event_date,
       max(c.event_date) as max_event_date
from spark.spark_event_activity_b c
join spark.spark_user_guid_map mp on mp.user_guid = c.user_guid
join warehouse.hana_dim_date dd on date(dd.date_date) = date(c.event_date)
where 1=1
and date(c.event_date) >= date('"""+date_from+"""')
and date(c.event_date) <= date('"""+date_to+"""')
and c.auth_flag = 'Y'
and c.event_name in ('Login Success','authentication:loginSucceeded')
group by c.user_guid, mp.original_guid, dd.fiscal_yr_and_per
"""
# ## umesto: and dd.fiscal_yr_and_per = """+current_fiscal_yr_and_per+"""
# ## and date(c.event_date) >= date('"""+date_from+"""')
# ## and date(c.event_date) <= date('"""+date_to+"""')

print("Date from:", date_from)
print("Date to:", date_to)

# Trino
df_query_spark_event_activity_logins = load_data_from_hadoop.get_hadoop_data(query_spark_event_activity_logins)

if not df_query_spark_event_activity_logins.empty:
    schema = "hdp"
    table = "spark_event_activity_b_logins"
    insert_data_to_local_database.insert_data_to_table(schema, table, df_query_spark_event_activity_logins, if_exists_="append")



# 2 content_detail
# koristi externu member guid map
# user_gk.edex_cc_usage
# hdp.ccmusg_fact_user_activity_cc_dc
max_activity_date = run_sql_query_local.get_data_from_sql_query_local_database("select max(date(max_activity_date)) from hdp.ccmusg_fact_user_activity_cc_dc;")
date_from = max_activity_date.iloc[0,0]
date_from = date_from + timedelta(days=1)
date_to = date.today() - timedelta(days=1) # two days ago
date_from = date_from.strftime('%Y-%m-%d')
date_to = date_to.strftime('%Y-%m-%d')

date_from = "2023-05-21"
date_to = "2023-05-25"

query_ccmusg_fact_user_activity = """
select b.member_guid as user_guid,
       b.category,
       case when b.category = 'HIGHBEAM_SESSION' then 'CC'
            when b.category = 'ACROBAT_SESSION' then 'DC'
       end as product_category,
       dd.fiscal_yr_and_per,
       min(date(b.activity_date)) as min_activity_date,
       max(date(b.activity_date)) as max_activity_date
from ccmusg.fact_user_activity b
join user_gk.member_guid_map mp on mp.userguid = b.member_guid
join warehouse.hana_dim_date dd on date(dd.date_date) = date(b.activity_date)
where b.category in ('HIGHBEAM_SESSION', 'ACROBAT_SESSION')
and lower(b.product) in ('acrobat','sparkler','spark','nimbus','lightroom classic','lightroom cc','character animator','rush','animate','prelude','audition',
'after effects','premiere pro','indesign','lightroom','dreamweaver','illustrator','photoshop','bridge','media encoder','dimension','incopy')
and date(b.activity_date) >= date('"""+date_from+"""')
and date(b.activity_date) <= date('"""+date_to+"""')
group by b.member_guid, b.category, dd.fiscal_yr_and_per
"""

# ## and dd.fiscal_yr_and_per = """+current_fiscal_yr_and_per+"""
# ## and date(b.activity_date) >= date('"""+date_from+"""')
# ## and date(b.activity_date) <= date('"""+date_to+"""')

print("Date from:", date_from)
print("Date to:", date_to)

# Trino
df_ccmusg_fact_user_activity = load_data_from_hadoop.get_hadoop_data(query_ccmusg_fact_user_activity)

if not df_ccmusg_fact_user_activity.empty:
    schema = "hdp"
    table = "ccmusg_fact_user_activity_cc_dc"
    insert_data_to_local_database.insert_data_to_table(schema, table, df_ccmusg_fact_user_activity, if_exists_="append")


#comparison_df = df_ccmusg_fact_user_activity.merge(df_ccmusg_fact_user_activity_1, indicator=True, on=["user_guid", "category", "product_category", "fiscal_yr_and_per"], how='outer')
#diff_df = comparison_df[comparison_df['_merge'] != 'both']

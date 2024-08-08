import sys
sys.path.append("import") # because load_dimension_data_table_v1 is in another directory
import core.utils.load_data_from_hadoop as load_data_from_hadoop
import core.utils.insert_data_to_local_database as insert_data_to_local_database
import core.utils.run_sql_query_local as run_sql_query_local
from datetime import date, timedelta


#current_fiscal_yr_and_per = run_sql_query_local.get_data_from_sql_query_local_database("select fiscal_yr_and_per from edx.hana_dim_date where calendar_date = current_date();")
#current_fiscal_yr_and_per = current_fiscal_yr_and_per.iloc[0,0]
#current_fiscal_yr_and_per = str(current_fiscal_yr_and_per)


# 1
# hdp.mcietl_web_visits_detailed_edex_clicks
max_click_date = run_sql_query_local.get_data_from_sql_query_local_database("select max(date(click_date)) from hdp.mcietl_web_visits_detailed_edex_clicks;")
date_from = max_click_date.iloc[0,0]
date_from = date_from + timedelta(days=1)
date_to = date.today() - timedelta(days=2) # two days ago
date_from = date_from.strftime('%Y-%m-%d')
date_to = date_to.strftime('%Y-%m-%d')

query_url_edex_adobe_com = """
select t.visid_high,
       t.visid_low,
       t.visit_num,
       t.click_date,
       year(t.click_date) as year,
       month(t.click_date) as month,
       t.date_time,
       sc.guid,
       t.page_url, -- dodao 24.01.2023
       dd.fiscal_yr_and_per
from mcietl.web_visits_detailed t
join warehouse.hana_dim_date dd on date(dd.calendar_date) = t.click_date
left join sourcedata.sc_visid_guid_unique sc on cast(sc.visid_high as bigint) = t.visid_high and cast(sc.visid_low as bigint) = t.visid_low
where t.page_url like '%edex.adobe.com%'
and t.report_suite ='adbadobenonacdcprod'
and t.click_date >= date('"""+date_from+"""')
and t.click_date <= date('"""+date_to+"""')
"""

# Trino
#df_edex = load_data_from_hadoop.get_hadoop_data(query_url_edex_adobe_com)

# Hive
df_edex = load_data_from_hadoop.get_hive_data(query_url_edex_adobe_com)

schema = "hdp"
table = "mcietl_web_visits_detailed_edex_clicks"
insert_data_to_local_database.insert_data_to_table(schema, table, df_edex, if_exists_="append")



# 2
# hdp.mcietl_web_visits_detailed_express_clicks
max_click_date = run_sql_query_local.get_data_from_sql_query_local_database("select max(date(click_date)) from hdp.mcietl_web_visits_detailed_express_clicks;")
date_from = max_click_date.iloc[0,0]
date_from = date_from + timedelta(days=1)
date_to = date.today() - timedelta(days=2) # two days ago
date_from = date_from.strftime('%Y-%m-%d')
date_to = date_to.strftime('%Y-%m-%d')

query_url_express_adobe_com = """
select t.*,sc.guid 
from (
select wb.visid_high, 
       wb.visid_low,
       wb.visit_num,
       dd.fiscal_yr_and_per,
       min(wb.click_date) as min_click_date,
       min(wb.date_time) as first_expresss_time,
       max(wb.date_time) as last_expresss_time
from mcietl.web_visits_detailed wb
join warehouse.hana_dim_date dd on date(dd.calendar_date) = wb.click_date
where wb.page_url like '%express.adobe.com%' -- (or wb.page_url like '%spark.adobe.com%')-- page_url like '%spark.adobe.com%' 2021-12 se pojavljuje i nazad, posle je express.adobe.com
    and wb.report_suite ='adbadobenonacdcprod'
    and wb.click_date >= date('"""+date_from+"""')
    and wb.click_date <= date('"""+date_to+"""')
    -- and wb.click_date != date('2022-12-14')
group by wb.visid_low, wb.visid_high, wb.visit_num, dd.fiscal_yr_and_per) t
left join sourcedata.sc_visid_guid_unique sc on cast(sc.visid_high as bigint) = t.visid_high and cast(sc.visid_low as bigint) = t.visid_low
"""

# Trino
#df_express = load_data_from_hadoop.get_hadoop_data(query_url_express_adobe_com)

# Hive
df_express = load_data_from_hadoop.get_hive_data(query_url_express_adobe_com)

schema = "hdp"
table = "mcietl_web_visits_detailed_express_clicks"
insert_data_to_local_database.insert_data_to_table(schema, table, df_express, if_exists_="append")



# 3 content_detail
# hdp.spark_event_activity_b_logins
max_event_date = run_sql_query_local.get_data_from_sql_query_local_database("select max(date(max_event_date)) from hdp.spark_event_activity_b_logins;")
date_from = max_event_date.iloc[0,0]
date_from = date_from + timedelta(days=1)
date_to = date.today() - timedelta(days=2) # two days ago
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

# Trino
df_query_spark_event_activity_logins = load_data_from_hadoop.get_hadoop_data(query_spark_event_activity_logins)

schema = "hdp"
table = "spark_event_activity_b_logins"
insert_data_to_local_database.insert_data_to_table(schema, table, df_query_spark_event_activity_logins, if_exists_="append")



# 4 content_detail
# koristi externu member guid map
# hdp.ccmusg_fact_user_activity_cc_dc
max_activity_date = run_sql_query_local.get_data_from_sql_query_local_database("select max(date(max_activity_date)) from hdp.ccmusg_fact_user_activity_cc_dc;")
date_from = max_activity_date.iloc[0,0]
date_from = date_from + timedelta(days=1)
date_to = date.today() - timedelta(days=2) # two days ago
date_from = date_from.strftime('%Y-%m-%d')
date_to = date_to.strftime('%Y-%m-%d')

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

# Trino
df_ccmusg_fact_user_activity = load_data_from_hadoop.get_hadoop_data(query_ccmusg_fact_user_activity)

schema = "hdp"
table = "ccmusg_fact_user_activity_cc_dc"
insert_data_to_local_database.insert_data_to_table(schema, table, df_ccmusg_fact_user_activity, if_exists_="append")

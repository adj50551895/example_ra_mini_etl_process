import sys
sys.path.append("import") # because load_dimension_data_table_v1 is in another directory
import core.utils.load_data_from_hadoop as load_data_from_hadoop
import core.utils.insert_data_to_local_database as insert_data_to_local_database
import core.utils.run_sql_query_local as run_sql_query_local
from datetime import date, timedelta


#current_fiscal_yr_and_per = run_sql_query_local.get_data_from_sql_query_local_database("select fiscal_yr_and_per from edx.hana_dim_date where calendar_date = current_date();")
#current_fiscal_yr_and_per = current_fiscal_yr_and_per.iloc[0,0]
#current_fiscal_yr_and_per = str(current_fiscal_yr_and_per)

#dash report

# 1
# hdp.mcietl_web_visits_detailed_edex_clicks
max_click_date = run_sql_query_local.get_data_from_sql_query_local_database("select max(date(click_date)) from hdp.mcietl_web_visits_detailed_edex_clicks;")
date_from = max_click_date.iloc[0,0]
date_from = date_from + timedelta(days=1)
date_to = date.today() - timedelta(days=1) # two days ago
date_from = date_from.strftime('%Y-%m-%d')
date_to = date_to.strftime('%Y-%m-%d')

#date_from = "2023-04-28"
#date_to = "2023-03-29"

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

print("Date from:", date_from)
print("Date to:", date_to)

# Trino
#df_edex = load_data_from_hadoop.get_hadoop_data(query_url_edex_adobe_com)

# Hive
df_edex = load_data_from_hadoop.get_hive_data(query_url_edex_adobe_com)

if not df_edex.empty:
    schema = "hdp"
    table = "mcietl_web_visits_detailed_edex_clicks"
    insert_data_to_local_database.insert_data_to_table(schema, table, df_edex, if_exists_="append")


del date_from
del date_to

# 2
# hdp.mcietl_web_visits_detailed_express_clicks
max_last_expresss_time = run_sql_query_local.get_data_from_sql_query_local_database("select max(date(last_expresss_time)) from hdp.mcietl_web_visits_detailed_express_clicks;")
date_from = max_last_expresss_time.iloc[0,0]
date_from = date_from + timedelta(days=1)
date_to = date.today() - timedelta(days=1) # two days ago
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
# ## and wb.click_date >= date('"""+date_from+"""')
# ## and wb.click_date <= date('"""+date_to+"""')

print("Date from:", date_from)
print("Date to:", date_to)

# Trino
#df_express = load_data_from_hadoop.get_hadoop_data(query_url_express_adobe_com)

# Hive
df_express = load_data_from_hadoop.get_hive_data(query_url_express_adobe_com)

if not df_express.empty:
    schema = "hdp"
    table = "mcietl_web_visits_detailed_express_clicks"
    insert_data_to_local_database.insert_data_to_table(schema, table, df_express, if_exists_="append")

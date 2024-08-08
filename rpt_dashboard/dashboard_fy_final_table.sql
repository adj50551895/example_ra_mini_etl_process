select t.fiscal_month_dt as "Month Description",
       DATE_FORMAT(date_sub(STR_TO_DATE(t.fiscal_month_dt, '%Y-%m-%d'), INTERVAL 1 MONTH), '%Y-%m') as "Year-Mon",
       t.class as institution_type,
       case when t.countryCode = 'US' then 'US'
            else t.countryCode_1 
       end as countryCode,
       t.countryCode as region_class,
       case when t.countryCode = 'US' then 'NOAM'
            when c.region = '' then 'UD'
            else ifnull(c.region, t.countryCode_1)
       end as region,
       -- c.country,
       case when t.sky_scraper = '1' then 'Sky Scraper'
            else 'Non-Sky Scraper'
       end as sky_scraper,
       case when t.top_500 = '1' then 'Top 500'
            else 'Non-Top 500'
       end as top_500,
       case when t.top_200 = '1' then 'Top 200'
            else 'Non-Top 200'
       end as top_200,
       case when t.top_100 = '1' then 'Top 100'
            else 'Non-Top 100'
       end as top_100,
       t.pod,
       case when t.ace = '1' then 'Yes'
            else 'No'
       end as ace,
       t.fiscal_yr_and_per,
       t.fiscal_yr_and_per_desc as "Month ID",
       t.fiscal_yr_and_qtr_desc as "Quarter Flag",
       t.current_month_flag as "Current Month Flag",
       t.current_quarter_flag as "Current Quarter Flag",
       t.current_year_flag as "Current Year Flag",
       t.last_month_flag as "Last Month Flag",
       t.last_year_flag as "Last Year Flag",
       t.last_quarter_flag as "Last Quarter Flag",
       t.same_quarter_last_year_flag as "Same Quarter Last Year Flag",
       t.past_quarters as "Quarters Past",
       t.past_months as "Months Past",
       t.running_current_month_flag,
       t.running_current_quarter_flag,
       t.unique_acquisition as "Unique user content acquisition",
       t.edex_new_user_signups as "User Sign Ups", -- new_user_signups
       t.total_acquisition as "Total Acquisition",
       t.courses_enrolled as "Courses enrolled in",
       t.resource_downloaded as "Resource downloads", -- resource_downloads
       t.new_user_content_acquisition as "New User Content acquisition",
       t.ret_user_content_acquisition as "Returning user content acquisition",
       t.unique_member_enrollment as "Unique Members who enrolled",
       t.new_unique_member_enrollment as "New Unique Members who enrolled",
       t.unique_member_downloads as "Unique Members who downloaded Resources",
       t.new_unique_member_downloads as "New Unique Members who downloaded Resources",
       t.new_member_courses_enrolled as "Courses enrolled in by new users",
       t.ret_member_courses_enrolled as "Courses enrolled in by returning users",
       t.new_member_resource_downloads as "Resources downloaded by New users",
       t.ret_member_resource_downloads as "Resources downloaded by Returning users",
       t.ret_unique_member_enrollment as "Returning Unique Members who enrolled",
       t.ret_unique_member_downloads as "Returning Unique Members who downloaded Resources",
       t.mau_total_members as "CCX MAU", -- ccx_mau
       t.mau_total_new_members as "New User CCX MAU", -- new_user_ccx_mau
       t.mau_total_returning_members as "Returning User CCX MAU" -- returning_user_ccx_mau
       ,0 as "Unique visitors"
       ,0 as "New Visitors"
       ,0 as "Unique members"
       ,0 as "Returning Visitor"
       ,0 as "Returning Members"
       ,(select p.param_value from edx.rpt_params p where p.report_name = 'rpt_dashboard' and p.param_name = 'refresh_date') as RefreshDate
from edx.dashboard_final_results t
left join edx.region_country_codes c on c.country_code = t.countryCode_1
where 1=1
-- and t.isCurrent = 1
order by 1,2,3,4,5,6,7,8;
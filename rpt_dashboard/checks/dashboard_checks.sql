select t.fiscal_month_dt,
       t.class,
       t.countryCode,
       sum(t.unique_acquisition) as unique_acquisition,
       sum(t.total_acquisition) as total_acquisition,
       sum(t.courses_enrolled) as courses_enrolled,
       sum(t.resource_downloaded) as resource_downloaded,
       sum(t.new_user_content_acquisition) as new_user_content_acquisition,
       sum(t.ret_user_content_acquisition) as ret_user_content_acquisition,
       sum(t.unique_member_enrollment) as unique_member_enrollment,
       sum(t.new_unique_member_enrollment) as new_unique_member_enrollment,
       sum(t.unique_member_downloads) as unique_member_downloads,
       sum(t.new_unique_member_downloads) as new_unique_member_downloads,
       sum(t.new_member_courses_enrolled) as new_member_courses_enrolled,
       sum(t.ret_member_courses_enrolled) as ret_member_courses_enrolled,
       sum(t.new_member_resource_downloads) as new_member_resource_downloads,
       sum(t.ret_member_resource_downloads) as ret_member_resource_downloads,
       sum(t.ret_unique_member_enrollment) as ret_unique_member_enrollment,
       sum(t.ret_unique_member_downloads) as ret_unique_member_downloads,
       --
       -- sum(t.mau_members) + sum(t.mau_visitors) as total_members1,
       -- sum(t.mau_new_members) + sum(t.mau_new_visitors) as total_new_members1,
       -- sum(t.mau_members) + sum(t.mau_visitors) - sum(t.mau_new_members) - sum(t.mau_new_visitors) as total_returning_members1,
       sum(t.mau_total_members) as mau_total_members,
	   sum(t.mau_total_new_members) as mau_total_new_members,
       sum(t.mau_total_returning_members) as mau_total_returning_members,
       --
       sum(t.mau_visitors) as mau_visitors,
       sum(t.mau_new_visitors) as mau_new_visitors,
       sum(t.mau_returning_visitors) as mau_returning_visitors,
       sum(t.mau_members) as mau_members,
       sum(t.edex_new_user_signups) as edex_new_user_signups
from ra.dashboard_final_results t -- ra.dashboard_final2 t
-- where t.fiscal_month_dt = '2022-10-01'
group by t.fiscal_month_dt,
         t.class,
         t.countryCode;
         
select distinct fiscal_month_dt
from ra.dashboard_final_results t
limit 10

-- unique_acquisition 6672
select t.class,
       t.countryCode,
       sum(t.unique_acquisition) as unique_acquisition
from ra.tmp_dashboard_unique_acquisition t
group by t.class,
         t.countryCode;

-- courses_enrolled 2518
select t.class,
       t.countryCode,
       sum(t.courses_enrolled) as courses_enrolled
from ra.tmp_dashboard_courses_enrolled t
group by t.class,
         t.countryCode;

-- resource_downloaded 10135
select t.class,
       t.countryCode,
       sum(t.resource_downloaded) as resource_downloaded
from ra.tmp_dashboard_resource_downloaded t
group by t.class,
         t.countryCode;

-- new_user_content_acquisition 4723
select t.class,
       t.countryCode,
       sum(t.new_user_content_acquisition) as new_user_content_acquisition
from ra.tmp_dashboard_new_user_content_acquisition t
group by t.class,
         t.countryCode;

-- ret_user_content_acquisition 1949
select t.class,
       t.countryCode,
       sum(t.ret_user_content_acquisition) as ret_user_content_acquisition
from ra.tmp_dashboard_ret_user_content_acquisition t
group by t.class,
         t.countryCode;

-- unique_member_enrollment 1819
select t.class,
       t.countryCode,
       sum(t.unique_member_enrollment) as unique_member_enrollment
from ra.tmp_dashboard_unique_member_enrollment t
group by t.class,
         t.countryCode;

-- new_unique_member_enrollment 1121
select t.class,
       t.countryCode,
       sum(t.new_unique_member_enrollment) as new_unique_member_enrollment
from ra.tmp_dashboard_new_unique_member_enrollment t
group by t.class,
         t.countryCode;

-- unique_member_downloads 5373
select t.class,
       t.countryCode,
       sum(t.unique_member_downloads) as unique_member_downloads
from ra.tmp_dashboard_unique_member_downloads t
group by t.class,
         t.countryCode;

-- new_unique_member_downloads 3910
select t.class,
       t.countryCode,
       sum(t.new_unique_member_downloads) as new_unique_member_downloads
from ra.tmp_dashboard_new_unique_member_downloads t
group by t.class,
         t.countryCode;

-- new_member_courses_enrolled 1514
select t.class,
       t.countryCode,
       sum(t.new_member_courses_enrolled) as new_member_courses_enrolled
from ra.tmp_dashboard_new_member_courses_enrolled t
group by t.class,
         t.countryCode;

-- ret_member_courses_enrolled 1004
select t.class,
       t.countryCode,
       sum(t.ret_member_courses_enrolled) as ret_member_courses_enrolled
from ra.tmp_dashboard_ret_member_courses_enrolled t
group by t.class,
         t.countryCode;

-- new_member_resource_downloads 5892
select t.class,
       t.countryCode,
       sum(t.new_member_resource_downloads) as new_member_resource_downloads
from ra.tmp_dashboard_new_member_resource_downloads t
group by t.class,
         t.countryCode;

-- ret_member_resource_downloads 7294
select t.class,
       t.countryCode,
       sum(t.ret_member_resource_downloads) as ret_member_resource_downloads
from ra.tmp_dashboard_ret_member_resource_downloads t
group by t.class,
         t.countryCode;

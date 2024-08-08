-- May contain fiscal months gap (@current_fiscal_yr_and_per)
-- If the date of the last start of the report and the current date of the start of the report are in a different fiscal month, it is necessary to start the report twice - for the last two fiscal months.

-- uses stage tables, check before run
-- (content_acquisition) els.stg_events_acquisition_content
-- (mau) els.stg_member_first_time_content_acquisition
-- (mau) hdp.stg_edex_express_dates

select param_value
into @current_fiscal_yr_and_per
from edx.rpt_params
where report_name = 'rpt_dashboard' 
and param_name = 'rpt_fiscal_month';

-- set @current_fiscal_yr_and_per = 202308;

drop table if exists ra.tmp_dashboard_base_members;
create table ra.tmp_dashboard_base_members as
select m.id as memberId,
       d.school_district_name,
       d.domain,
       case when d.sky_scraper = '0' then '0'
            when d.sky_scraper is null then '0'
            else '1'
       end as sky_scraper,
       case when d.top_500 = '0' then '0'
            when d.top_500 is null then '0'
            else '1'
       end as top_500,
       case when d.top_200 = '0' then '0'
            when d.top_200 is null then '0'
            else '1'
       end as top_200,
       case when d.top_100 = '0' then '0'
            when d.top_100 is null then '0'
            else '1'
       end as top_100,
       case when d.pod = '0' then '0'
            when d.pod is null then '0'
            else d.pod
       end as pod,
       case when b.memberId is not null then '1' 
            else '0'
       end as ace,
       trim(mp.userGuid) as userGuid,
       m.createdAt as memberCreatedAt,
       case when m.countryCode = 'US' then m.countryCode
            when l.countryCode = 'US' and (m.countryCode = 'UD' OR m.countryCode = '' OR m.countryCode is null) then l.countryCode
            when (m.countryCode != 'US' and m.countryCode != 'UD' and length(m.countryCode) > 1) then 'ROW'
            when m.countryCode = '' then null
            else m.countryCode
       end as countryCode,
       case when l.countryCode = 'US' and (m.countryCode = 'UD' OR m.countryCode = '' OR m.countryCode is null) then l.countryCode            
            when m.countryCode = '' then null
	 	    else m.countryCode
       end as countryCode_1,
       s.class,
       dd.fiscal_yr_and_per
from edx.Member m
join hdp.memberguidmap mp on mp.memberId = m.id
left join edx.hana_dim_date dd on dd.calendar_date = date(m.createdAt)
left join edx.district_domains d on d.domain COLLATE utf8mb4_general_ci = lower(SUBSTRING_INDEX(m.email, "@", -1))
left join edx.ud_lookup l on l.memberId = m.id
left join edx.MemberSegmentation s on s.memberId = m.id
left join edx.memberToBadge b on b.memberId = m.id;

-- CONTENT ACQUISITION --------------------------------------------------------------------------------------------------------------
-- drop table if exists ra.tmp_dashboard_content_acquisition;
-- create table ra.tmp_dashboard_content_acquisition as
delete from ra.tmp_dashboard_content_acquisition where fiscal_yr_and_per = @current_fiscal_yr_and_per;

insert into ra.tmp_dashboard_content_acquisition
select t.id, -- concat(enr.memberId, enr.courseId)
       t.event_date,
       t.memberId,
       t.event,
       t.eventLevel,
       t.entityType,
       t.entityId,
       t.sessionId,
       t.link_type,
       d.fiscal_yr_and_per,
       DATE(CONCAT(d.fiscal_yr_and_per_desc,'-01')) as fiscal_month_dt,
       case when t.memberId is null then t.sessionId else t.memberId end as userId, -- concat(memberId and courseId), if null then sessionId
       case when m.memberId is null then 0 else 1 end as isMember,
       ROW_NUMBER() OVER (PARTITION BY d.fiscal_yr, case when t.memberId is null then t.sessionId else t.memberId end order by d.fiscal_yr, DATE(CONCAT(d.fiscal_yr_and_per_desc,'-01'))) as event_num,
       case when m.memberId is null then 'session_new'
            when m.fiscal_yr_and_per = d.fiscal_yr_and_per then 'new_event_date'
            when m.fiscal_yr_and_per = @current_fiscal_yr_and_per then 'new'
            -- when m.fiscal_yr_and_per = EXTRACT(YEAR_MONTH FROM @current_fiscal_yr_and_per) then 'new' -- for testing current month
            else 'returning'
       end as new_member,
       m.countryCode,
       m.countryCode_1,
       s.class,
       m.sky_scraper,
       m.top_500,
       m.top_200,
       m.top_100,
       m.pod,
       m.ace,
       m.fiscal_yr_and_per as member_fiscal_yr_and_per,
       m.memberCreatedAt
from els.stg_events_acquisition_content t
join edx.hana_dim_date d on d.calendar_date = date(t.event_date)
left join ra.tmp_dashboard_base_members m on m.memberId = t.memberId
left join edx.MemberSegmentation s on s.memberId = t.memberId
where 1=1
and t.link_type is not null -- ne vezi, otkomentarisano, (20.04.2023 izjednacavanje sa districtom, zakomentarisano)
and (case when t.memberId is null then 0 else 1 end) + (case when t.event in ('v1.engaged.resourceLink', 'resource.click.inlineLink', 'resource.click.weblink') then 0 else 1 end) >= 1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per;

-- postavi sve koji nemaju class na null
update ra.tmp_dashboard_content_acquisition set countryCode = null where length(countryCode) < 2;

-- update sessions set countryCode and class
drop table if exists ra.tmp_dashboard_update_sessions;
create table ra.tmp_dashboard_update_sessions as
select t.id as id,
       case when t.rn <= 60 then 'US'
            when t.rn >= 61 then 'ROW'
            else 'group 3' 
        end as country_code,
        case when t.rn <= 49 then 'K12'
            when t.rn between 50 and 60 then 'HED'
            when t.rn between 61 and 92 then 'K12'
            when t.rn >= 93 then 'HED'
        end as class
from (select f.id, NTILE(100) OVER (ORDER BY f.id) as rn from ra.tmp_dashboard_content_acquisition f where f.memberId is null) t;

update ra.tmp_dashboard_content_acquisition e set e.class = (select t.class from ra.tmp_dashboard_update_sessions t where t.id = e.id),
                                                  e.countryCode = (select t.country_code from ra.tmp_dashboard_update_sessions t where t.id = e.id)
where memberId is null;

-- update countryCode za null
drop table if exists ra.tmp_dashboard_update_sessions;
create table ra.tmp_dashboard_update_sessions as
select t.id as id,
       case when t.rn <= 60 then 'US'
            when t.rn >= 61 then 'ROW'
            else 'group 3' 
        end as country_code
from (select f.id, NTILE(100) OVER (ORDER BY f.id) as rn from ra.tmp_dashboard_content_acquisition f where f.countryCode is null and f.class is not null) t;

update ra.tmp_dashboard_content_acquisition e set e.countryCode = (select t.country_code from ra.tmp_dashboard_update_sessions t where t.id = e.id)
where countryCode is null and class is not null;

drop table if exists ra.tmp_dashboard_update_sessions;


-- unique_acquisition
-- 12491, 12503 count(distinct t.userId) unique_acquisition, old pre order by 12700
-- 6670    K12    US, FY 2022-09-01
drop temporary table if exists ra.tmp_dashboard_unique_acquisition;
create temporary table ra.tmp_dashboard_unique_acquisition as 
select t.fiscal_month_dt,
       count(distinct t.userId, entityID) as unique_acquisition,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.isMember = 1
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

-- Sessions, unique_acquisition
insert into ra.tmp_dashboard_unique_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId, entityID) * 0.595 * 0.8) as unique_acquisition,
       'K12' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_unique_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId, entityID) * 0.595 * 0.2) as unique_acquisition,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_unique_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId, entityID) * 0.405 * 0.8) as unique_acquisition,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_unique_acquisition
select t.fiscal_month_dt, 
       round(count(distinct t.userId, entityID) * 0.405 * 0.2) as unique_acquisition,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.isMember = 0
group by t.fiscal_month_dt;
-- //unique_acquisition


-- courses_enrolled
-- 6333, 6333
drop temporary table if exists ra.tmp_dashboard_courses_enrolled;
create temporary table ra.tmp_dashboard_courses_enrolled as 
select t.fiscal_month_dt,
       count(distinct t.userId, t.entityId) as courses_enrolled,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 1
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

-- Sessions, courses_enrolled
insert into ra.tmp_dashboard_courses_enrolled
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.8) as courses_enrolled,
       'K12' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_courses_enrolled
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.2) as courses_enrolled,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_courses_enrolled
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.8) as courses_enrolled,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_courses_enrolled
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.2) as courses_enrolled,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 0
group by t.fiscal_month_dt;


-- resource_downloaded
-- proveriti link_type Link
-- 17714, 17726
drop temporary table if exists ra.tmp_dashboard_resource_downloaded;
create temporary table ra.tmp_dashboard_resource_downloaded as 
select t.fiscal_month_dt,
       count(distinct t.userId, t.entityId) as resource_downloaded,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 1
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

-- US Sessions, resource_downloaded
insert into ra.tmp_dashboard_resource_downloaded
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.8) as resource_downloaded,
       'K12' as class,
       'US'as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_resource_downloaded
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.2) as resource_downloaded,
       'HED' as class,
       'US'as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_resource_downloaded
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.8) as resource_downloaded,
       'K12' as class,
       'ROW'as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_resource_downloaded
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.2) as resource_downloaded,
       'HED' as class,
       'ROW'as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 0
group by t.fiscal_month_dt;

-- new_user_content_acquisition
-- 10280, 10389
drop temporary table if exists ra.tmp_dashboard_new_user_content_acquisition;
create temporary table ra.tmp_dashboard_new_user_content_acquisition as 
select t.fiscal_month_dt,
       count(distinct t.userId) as new_user_content_acquisition,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member != 'returning'
and t.isMember = 1
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

insert into ra.tmp_dashboard_new_user_content_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.8) as new_user_content_acquisition,
       'K12' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_user_content_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.2) as new_user_content_acquisition,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_user_content_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.8) as new_user_content_acquisition,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_user_content_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.2) as new_user_content_acquisition,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;
-- //new_user_content_acquisition

-- ret_user_content_acquisition
-- 2211, 2114
drop temporary table if exists ra.tmp_dashboard_ret_user_content_acquisition;
create temporary table ra.tmp_dashboard_ret_user_content_acquisition as 
select t.fiscal_month_dt,
       count(distinct t.userId) as ret_user_content_acquisition,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member = 'returning'
and t.isMember = 1
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

-- US SessionID, ret_user_content_acquisition
insert into ra.tmp_dashboard_ret_user_content_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.8) as ret_user_content_acquisition,
       'K12' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member = 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_ret_user_content_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.2) as ret_user_content_acquisition,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member = 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_ret_user_content_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.8) as ret_user_content_acquisition,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member = 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_ret_user_content_acquisition
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.2) as ret_user_content_acquisition,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.event_num = '1'
and t.new_member = 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;
-- //ret_user_content_acquisition


-- unique_member_enrollment
-- 4513, 4513
drop temporary table if exists ra.tmp_dashboard_unique_member_enrollment;
create temporary table ra.tmp_dashboard_unique_member_enrollment as 
select t.fiscal_month_dt,
       count(distinct t.userId) as unique_member_enrollment,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 1
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

insert into ra.tmp_dashboard_unique_member_enrollment
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.8) as unique_member_enrollment,
       'K12'as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_unique_member_enrollment
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.2) as unique_member_enrollment,
       'HED'as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_unique_member_enrollment
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.8) as unique_member_enrollment,
       'K12'as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_unique_member_enrollment
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.2) as unique_member_enrollment,
       'HED'as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.isMember = 0
group by t.fiscal_month_dt;
-- //unique_member_enrollment

-- new_unique_member_enrollment
-- 3042, 3050
drop temporary table if exists ra.tmp_dashboard_new_unique_member_enrollment;
create temporary table ra.tmp_dashboard_new_unique_member_enrollment as 
select t.fiscal_month_dt,
       count(distinct t.userId) as new_unique_member_enrollment,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.new_member != 'returning'
and t.isMember = 1
-- and t.acq_num = '1'
-- and t.event_num = '1'
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

insert into ra.tmp_dashboard_new_unique_member_enrollment
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.8) as new_unique_member_enrollment,
       'K12' as class,
       'US' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.new_member != 'returning'
and t.isMember = 0
-- and t.acq_num = '1'
-- and t.event_num = '1'
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_unique_member_enrollment
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.2) as new_unique_member_enrollment,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.new_member != 'returning'
and t.isMember = 0
-- and t.acq_num = '1'
-- and t.event_num = '1'
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_unique_member_enrollment
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.8) as new_unique_member_enrollment,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.new_member != 'returning'
and t.isMember = 0
-- and t.acq_num = '1'
-- and t.event_num = '1'
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_unique_member_enrollment
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.2) as new_unique_member_enrollment,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
and t.new_member != 'returning'
and t.isMember = 0
-- and t.acq_num = '1'
-- and t.event_num = '1'
group by t.fiscal_month_dt;
-- //new_unique_member_enrollment

-- unique_member_downloads
-- 10263, 10275
drop temporary table if exists ra.tmp_dashboard_unique_member_downloads;
create temporary table ra.tmp_dashboard_unique_member_downloads as 
select t.fiscal_month_dt,
       count(distinct t.userId) as unique_member_downloads,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 1
-- and t.acq_num = '1'
-- and t.new_member != 'returning'
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

insert into ra.tmp_dashboard_unique_member_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.8) as unique_member_downloads,
       'K12' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 0
-- and t.acq_num = '1'
-- and t.new_member != 'returning'
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_unique_member_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.2) as unique_member_downloads,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 0
-- and t.acq_num = '1'
-- and t.new_member != 'returning'
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_unique_member_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.8) as unique_member_downloads,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 0
-- and t.acq_num = '1'
-- and t.new_member != 'returning'
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_unique_member_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.2) as unique_member_downloads,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.isMember = 0
-- and t.acq_num = '1'
-- and t.new_member != 'returning'
group by t.fiscal_month_dt;
-- //unique_member_downloads

-- new_unique_member_downloads
drop temporary table if exists ra.tmp_dashboard_new_unique_member_downloads;
create temporary table ra.tmp_dashboard_new_unique_member_downloads as 
select t.fiscal_month_dt,
       count(distinct t.userId) as new_unique_member_downloads,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
-- and t.acq_num = '1'
and t.new_member != 'returning'
and t.isMember = 1
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

insert into ra.tmp_dashboard_new_unique_member_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.8) as new_unique_member_downloads,
       'K12' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
-- and t.acq_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_unique_member_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.595 * 0.2) as new_unique_member_downloads,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
-- and t.acq_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_unique_member_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.8) as new_unique_member_downloads,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
-- and t.acq_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_unique_member_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId) * 0.405 * 0.2) as new_unique_member_downloads,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
-- and t.acq_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;
-- //new_unique_member_downloads

-- new_member_courses_enrolled
drop temporary table if exists ra.tmp_dashboard_new_member_courses_enrolled;
create temporary table ra.tmp_dashboard_new_member_courses_enrolled as
select t.fiscal_month_dt,
       count(distinct t.userId, t.entityId) as new_member_courses_enrolled,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
-- and t.acq_num = '1'
and t.new_member != 'returning'
-- and t.isMember = 1
-- and t.userId = '0056be24-a108-4521-844a-d9e7ade06f2c' -- '82303c61-6102-11e2-9a53-12313b016471'
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

insert into ra.tmp_dashboard_new_member_courses_enrolled
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.8) as new_member_courses_enrolled,
       'K12' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
-- and t.acq_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_member_courses_enrolled
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.2) as new_member_courses_enrolled,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
-- and t.acq_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_member_courses_enrolled
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.8) as new_member_courses_enrolled,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
-- and t.acq_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_member_courses_enrolled
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.2) as new_member_courses_enrolled,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
-- and t.acq_num = '1'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

-- ret_member_courses_enrolled
drop temporary table if exists ra.tmp_dashboard_ret_member_courses_enrolled;
create temporary table ra.tmp_dashboard_ret_member_courses_enrolled as
select t.fiscal_month_dt,
       count(distinct t.userId, t.entityId) as ret_member_courses_enrolled,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'course'
-- and t.acq_num = '1'
and t.new_member = 'Returning'
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;
-- //new_member_courses_enrolled

-- new_member_resource_downloads
drop temporary table if exists ra.tmp_dashboard_new_member_resource_downloads;
create temporary table ra.tmp_dashboard_new_member_resource_downloads as
select t.fiscal_month_dt,
       count(distinct t.userId, t.entityId) as new_member_resource_downloads,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member != 'returning'
and t.isMember = 1
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

insert into ra.tmp_dashboard_new_member_resource_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.8) as new_member_resource_downloads,
       'K12' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_member_resource_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.2) as new_member_resource_downloads,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_member_resource_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.8) as new_member_resource_downloads,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_new_member_resource_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.2) as new_member_resource_downloads,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member != 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;
-- //new_member_resource_downloads

-- ret_member_resource_downloads
drop temporary table if exists ra.tmp_dashboard_ret_member_resource_downloads;
create temporary table ra.tmp_dashboard_ret_member_resource_downloads as
select t.fiscal_month_dt,
       count(distinct t.userId, t.entityId) as ret_member_resource_downloads,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member = 'returning'
and t.isMember = 1
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;

insert into ra.tmp_dashboard_ret_member_resource_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.8) as ret_member_resource_downloads,
       'K12' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member = 'returning'
and t.isMember = 1
group by fiscal_month_dt;

insert into ra.tmp_dashboard_ret_member_resource_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.595 * 0.2) as ret_member_resource_downloads,
       'HED' as class,
       'US' as countryCode,
       'US' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member = 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_ret_member_resource_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.8) as ret_member_resource_downloads,
       'K12' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member = 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;

insert into ra.tmp_dashboard_ret_member_resource_downloads
select t.fiscal_month_dt,
       round(count(distinct t.userId, t.entityId) * 0.405 * 0.2) as ret_member_resource_downloads,
       'HED' as class,
       'ROW' as countryCode,
       'UD' as countryCode_1,
       '0' as sky_scraper,
       '0' as top_500,
       '0' as top_200,
       '0' as top_100,
       '0' as pod,
       '0' as ace,
       0 as member_flag
from ra.tmp_dashboard_content_acquisition t
where 1=1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
and t.link_type = 'resource'
and t.new_member = 'returning'
and t.isMember = 0
group by t.fiscal_month_dt;
-- //ret_member_resource_downloads

--
update ra.tmp_dashboard_unique_acquisition set class = 'null' where class is null;
update ra.tmp_dashboard_unique_acquisition set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_unique_acquisition set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_courses_enrolled set class = 'null' where class is null;
update ra.tmp_dashboard_courses_enrolled set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_courses_enrolled set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_resource_downloaded set class = 'null' where class is null;
update ra.tmp_dashboard_resource_downloaded set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_resource_downloaded set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_new_user_content_acquisition set class = 'null' where class is null;
update ra.tmp_dashboard_new_user_content_acquisition set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_new_user_content_acquisition set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_ret_user_content_acquisition set class = 'null' where class is null;
update ra.tmp_dashboard_ret_user_content_acquisition set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_ret_user_content_acquisition set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_unique_member_enrollment set class = 'null' where class is null;
update ra.tmp_dashboard_unique_member_enrollment set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_unique_member_enrollment set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_new_unique_member_enrollment set class = 'null' where class is null;
update ra.tmp_dashboard_new_unique_member_enrollment set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_new_unique_member_enrollment set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_unique_member_downloads set class = 'null' where class is null;
update ra.tmp_dashboard_unique_member_downloads set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_unique_member_downloads set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_new_unique_member_downloads set class = 'null' where class is null;
update ra.tmp_dashboard_new_unique_member_downloads set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_new_unique_member_downloads set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_new_member_courses_enrolled set class = 'null' where class is null;
update ra.tmp_dashboard_new_member_courses_enrolled set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_new_member_courses_enrolled set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_ret_member_courses_enrolled set class = 'null' where class is null;
update ra.tmp_dashboard_ret_member_courses_enrolled set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_ret_member_courses_enrolled set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_new_member_resource_downloads set class = 'null' where class is null;
update ra.tmp_dashboard_new_member_resource_downloads set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_new_member_resource_downloads set countryCode_1 = 'null' where countryCode_1 is null;

update ra.tmp_dashboard_ret_member_resource_downloads set class = 'null' where class is null;
update ra.tmp_dashboard_ret_member_resource_downloads set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_ret_member_resource_downloads set countryCode_1 = 'null' where countryCode_1 is null;
-- //CONTENT ACQUISITION --------------------------------------------------------------------------------------------------------------

-- MAU ------------------------------------------------------------------------------------------------------------------------------
-- edex members aquired content i nakon toga otisli na edex, logovali se na express
-- edex visitors posetili edex, otili na express i onda nakon toga se logovali na express _b

-- MAU Members
delete from ra.tmp_dashboard_mau where member_flag = 1 and fiscal_yr_and_per = @current_fiscal_yr_and_per;
insert into ra.tmp_dashboard_mau
select b.fiscal_yr_and_per,
       date(concat(left(b.fiscal_yr_and_per, 4), '-', right(b.fiscal_yr_and_per, 2), '-01')) as fiscal_month_dt,
       m.memberId,
       bm.class,
       bm.countryCode,
       bm.countryCode_1,
       bm.sky_scraper,
       bm.top_500,
       bm.top_200,
       bm.top_100,
       bm.pod,
       bm.ace,
       case when m.memberId is null then 'new_session'
            when bm.fiscal_yr_and_per = b.fiscal_yr_and_per then 'new_event_date'
            when bm.fiscal_yr_and_per = @current_fiscal_yr_and_per then 'new'
            -- when bm.fiscal_yr_and_per = EXTRACT(YEAR_MONTH FROM '2022-09-01') then 'new' -- testiramo 9. mesec
            else 'returning'
       end as new_member,
       1 as member_flag
from hdp.spark_event_activity_b_logins b
join hdp.memberguidmap m on m.userGuid COLLATE utf8mb4_general_ci = b.original_guid
join ra.tmp_dashboard_base_members bm on bm.userGuid = b.original_guid COLLATE utf8mb4_general_ci
where exists (
    select 1 
    from els.stg_member_first_time_content_acquisition t 
    where t.memberId = m.memberId
    and t.fiscal_yr_and_per <= b.fiscal_yr_and_per
)
and b.fiscal_yr_and_per = @current_fiscal_yr_and_per;


-- MAU Visitors
delete from ra.tmp_dashboard_mau where member_flag = 0 and fiscal_yr_and_per = @current_fiscal_yr_and_per;
insert into ra.tmp_dashboard_mau
select ee.edx_fiscal_yr_and_per,
       date(concat(left(ee.edx_fiscal_yr_and_per, 4), '-', right(ee.edx_fiscal_yr_and_per, 2), '-01')) as fiscal_month_dt,
       ee.guid,
       bm.class,
       bm.countryCode,
       bm.countryCode_1,
       bm.sky_scraper,
       bm.top_500,
       bm.top_200,
       bm.top_100,
       bm.pod,
       bm.ace,
       case when bm.memberId is null then 'session_new'
            when bm.fiscal_yr_and_per = ee.edx_fiscal_yr_and_per then 'new_event_date'
            when bm.fiscal_yr_and_per = @current_fiscal_yr_and_per then 'new'
            -- when bm.fiscal_yr_and_per = EXTRACT(YEAR_MONTH FROM '2022-09-01') then 'new2' -- testiramo 9. mesec
            else 'returning'
       end as new_member,
       0 as member_flag
from hdp.stg_edex_express_dates ee
left join ra.tmp_dashboard_base_members bm on bm.userGuid = ee.guid COLLATE utf8mb4_general_ci
where 1=1
and exists (
    select 1 from hdp.spark_event_activity_b_logins t 
    where t.original_guid = ee.guid
    and t.max_event_date between ee.edx_date and (ee.edx_date + interval 35 day)
)
and ee.edx_fiscal_yr_and_per = @current_fiscal_yr_and_per;

-- updating null vaules with 'null' strings
update ra.tmp_dashboard_mau set class = 'null' where class is null;
update ra.tmp_dashboard_mau set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_mau set countryCode_1 = 'null' where countryCode_1 is null;
update ra.tmp_dashboard_mau set sky_scraper = 'null' where sky_scraper is null;
update ra.tmp_dashboard_mau set top_500 = 'null' where top_500 is null;
update ra.tmp_dashboard_mau set top_200 = 'null' where top_200 is null;
update ra.tmp_dashboard_mau set top_100 = 'null' where top_100 is null;
update ra.tmp_dashboard_mau set pod = 'null' where pod is null;
update ra.tmp_dashboard_mau set ace = 'null' where ace is null;


drop temporary table if exists ra.tmp_dashboard_mau_members;
create temporary table ra.tmp_dashboard_mau_members as
select t.fiscal_month_dt,
       count(distinct t.memberId) as mau_members,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       t.member_flag
from ra.tmp_dashboard_mau t
where t.member_flag = 1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace,
         t.member_flag;

drop temporary table if exists ra.tmp_dashboard_mau_new_members;
create temporary table ra.tmp_dashboard_mau_new_members as
select t.fiscal_month_dt,
       count(distinct t.memberId) as mau_new_members,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       t.member_flag
from ra.tmp_dashboard_mau t
where t.member_flag = 1
and t.new_member != 'returning'
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace,
         t.member_flag;

drop temporary table if exists ra.tmp_dashboard_mau_returning_members;
create temporary table ra.tmp_dashboard_mau_returning_members as
select t.fiscal_month_dt,
       count(distinct t.memberId) as mau_returning_members,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       t.member_flag
from ra.tmp_dashboard_mau t
where t.member_flag = 1
and t.new_member = 'returning'
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace,
         t.member_flag;

drop temporary table if exists ra.tmp_dashboard_mau_visitors;
create temporary table ra.tmp_dashboard_mau_visitors as
select t.fiscal_month_dt,
       count(distinct t.memberId) as mau_visitors,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       t.member_flag
from ra.tmp_dashboard_mau t
where t.member_flag = 0
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace,
         t.member_flag;

drop temporary table if exists ra.tmp_dashboard_mau_new_visitors;
create temporary table ra.tmp_dashboard_mau_new_visitors as
select t.fiscal_month_dt,
       count(distinct t.memberId) as mau_new_visitors,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       t.member_flag
from ra.tmp_dashboard_mau t
where t.member_flag = 0
and t.new_member != 'returning'
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace,
         t.member_flag;

drop temporary table if exists ra.tmp_dashboard_mau_returning_visitors;
create temporary table ra.tmp_dashboard_mau_returning_visitors as
select t.fiscal_month_dt,
       count(distinct t.memberId) as mau_returning_visitors,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       t.member_flag
from ra.tmp_dashboard_mau t
where t.member_flag = 0
and t.new_member = 'returning'
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per
group by t.fiscal_month_dt,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace,
         t.member_flag;
-- //MAU ------------------------------------------------------------------------------------------------------------------------------


-- //Edex ------------------------------------------------------------------------------------------------------------------------------
drop temporary table if exists ra.tmp_dashboard_edex_new_user_signups;
create temporary table ra.tmp_dashboard_edex_new_user_signups as
select date(concat(left(t.fiscal_yr_and_per, 4), '-', right(t.fiscal_yr_and_per, 2), '-01')) as fiscal_month_dt,
       count(*) as edex_new_user_signups,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       1 as member_flag
from ra.tmp_dashboard_base_members t
join edx.hana_dim_date dd on dd.calendar_date = date(t.memberCreatedAt)
where dd.fiscal_yr_and_per = @current_fiscal_yr_and_per
group by t.fiscal_yr_and_per,
         t.class,
         t.countryCode,
         t.countryCode_1,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.ace;


update ra.tmp_dashboard_edex_new_user_signups set class = 'null' where class is null;
update ra.tmp_dashboard_edex_new_user_signups set countryCode = 'null' where countryCode is null;
update ra.tmp_dashboard_edex_new_user_signups set countryCode_1 = 'null' where countryCode_1 is null;
update ra.tmp_dashboard_edex_new_user_signups set countryCode_1 = 'null' where countryCode_1 = '';
update ra.tmp_dashboard_edex_new_user_signups set sky_scraper = 'null' where sky_scraper is null;
update ra.tmp_dashboard_edex_new_user_signups set top_500 = 'null' where top_500 is null;
update ra.tmp_dashboard_edex_new_user_signups set top_200 = 'null' where top_200 is null;
update ra.tmp_dashboard_edex_new_user_signups set top_100 = 'null' where top_100 is null;
update ra.tmp_dashboard_edex_new_user_signups set pod = 'null' where pod is null;
update ra.tmp_dashboard_edex_new_user_signups set ace = 'null' where ace is null;
-- //Edex ------------------------------------------------------------------------------------------------------------------------------

-- Adobe dates -------------------------------------------------------------------------------------------------------------------------
drop temporary table if exists ra.tmp_dashboard_adobe_quarters;
create temporary table ra.tmp_dashboard_adobe_quarters as
select dd.fiscal_yr_and_qtr_key,
       dd.fiscal_yr_and_qtr_desc,
       min(dd.calendar_date) as first_day, 
       max(dd.calendar_date) as last_day,
       ROW_NUMBER() OVER(order BY dd.fiscal_yr_and_qtr_key desc) - 1 AS past_quarters
from edx.hana_dim_date dd
where dd.fiscal_yr_and_qtr_key <= (select t.fiscal_yr_and_qtr_key from edx.hana_dim_date t where t.calendar_date = current_date())
and dd.fiscal_yr_and_qtr_key != 0
group by dd.fiscal_yr_and_qtr_key,
         dd.fiscal_yr_and_qtr_desc
order by dd.fiscal_yr_and_qtr_key desc;

drop temporary table if exists ra.tmp_dashboard_adobe_months;
create temporary table ra.tmp_dashboard_adobe_months as
select dd.fiscal_yr_and_per,
       dd.fiscal_yr_and_per_desc,
       min(dd.calendar_date) as first_day, 
       max(dd.calendar_date) as last_day,
       (TIMESTAMPDIFF(MONTH, (date(concat(left(dd.fiscal_yr_and_per, 4), '-', right(dd.fiscal_yr_and_per, 2), '-01'))), CURRENT_DATE())) as past_months
       -- ROW_NUMBER() OVER(order BY dd.fiscal_yr_and_per desc) - 1 AS past_months
from edx.hana_dim_date dd
where dd.fiscal_yr_and_per <= (select t.fiscal_yr_and_per from edx.hana_dim_date t where t.calendar_date = current_date())
and dd.fiscal_yr_and_qtr_key != 0
group by dd.fiscal_yr_and_per,
         dd.fiscal_yr_and_per_desc
order by dd.fiscal_yr_and_per desc;

drop temporary table if exists ra.tmp_dashboard_adobe_fy_dates;
create temporary table ra.tmp_dashboard_adobe_fy_dates as
select distinct 
       dd.fiscal_yr_and_per,
       dd.fiscal_yr_and_per_desc,
       dd.fiscal_yr_and_qtr_desc,
       date(concat(left(dd.fiscal_yr_and_per, 4), '-', right(dd.fiscal_yr_and_per, 2), '-01')) as fiscal_month_dt,
       case when dd.fiscal_yr_and_per = EXTRACT(YEAR_MONTH FROM CURRENT_DATE()) then '1'
            else '0'
       end as current_month_flag,
       case when q.past_quarters = 1 then '1'
            else '0'
       end as current_quarter_flag,
       case when dd.fiscal_yr = EXTRACT(YEAR FROM CURRENT_DATE()) then '1'
            else '0'
       end as current_year_flag,
       -- dd.las as last_month_flag       
       case when (TIMESTAMPDIFF(MONTH, (date(concat(left(dd.fiscal_yr_and_per, 4), '-', right(dd.fiscal_yr_and_per, 2), '-01'))), CURRENT_DATE())) = 1 then '1'
            else '0'
       end as last_month_flag,
       case when (TIMESTAMPDIFF(MONTH, (date(concat(left(dd.fiscal_yr_and_per, 4), '-', right(dd.fiscal_yr_and_per, 2), '-01'))), CURRENT_DATE())) = 12 then '1'
            else '0'
       end as last_year_flag,
       case when q.past_quarters = 2 then '1'
            else '0'
       end as last_quarter_flag,
       case when q.past_quarters = 5 then '1'
            else '0'
       end as same_quarter_last_year_flag,
       q.past_quarters,
       m.past_months,
       case when dd.fiscal_yr_and_per = (select t1.fiscal_yr_and_per from edx.hana_dim_date t1 where t1.calendar_date = current_date()) then '1'
            else '0'
       end as "running_current_month_flag",
       case when dd.fiscal_yr_and_qtr_desc = (select t1.fiscal_yr_and_qtr_desc from edx.hana_dim_date t1 where t1.calendar_date = current_date()) then '1'
            else '0'
       end as "running_current_quarter_flag"
from edx.hana_dim_date dd
join ra.tmp_dashboard_adobe_quarters q on q.fiscal_yr_and_qtr_key = dd.fiscal_yr_and_qtr_key
join ra.tmp_dashboard_adobe_months m on m.fiscal_yr_and_per = dd.fiscal_yr_and_per;
-- where dd.fiscal_yr_and_per = @current_fiscal_yr_and_per;
-- //Adobe dates -----------------------------------------------------------------------------------------------------------------------


-- creating tmp_dashboard_final_base combination table
drop table if exists ra.tmp_dashboard_final_base;
create table ra.tmp_dashboard_final_base as
select t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_unique_acquisition t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_courses_enrolled t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_resource_downloaded t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_new_user_content_acquisition t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_ret_user_content_acquisition t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_unique_member_enrollment t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_new_unique_member_enrollment t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_unique_member_downloads t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_new_unique_member_downloads t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_new_member_courses_enrolled t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_ret_member_courses_enrolled t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_new_member_resource_downloads t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_ret_member_resource_downloads t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_mau_members t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_mau_new_members t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_mau_returning_members t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_mau_visitors t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_mau_new_visitors t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_mau_returning_visitors t
union
select distinct t.fiscal_month_dt, t.class, t.countryCode, t.countryCode_1, t.sky_scraper, t.top_500, t.top_200, t.top_100, t.pod, t.ace, t.member_flag
from ra.tmp_dashboard_edex_new_user_signups t;

-- used in final table
update edx.rpt_params
set param_value = (select max(event_date) from ra.tmp_dashboard_content_acquisition t),
    dateUpdated = CURRENT_TIMESTAMP()
where report_name = 'rpt_dashboard'
and param_name = 'refresh_date';

delete from edx.dashboard_final_results where fiscal_yr_and_per = @current_fiscal_yr_and_per;

select ifNull(max(t.population), 0)
into @population
from edx.dashboard_final_results t;

set @population = @population + 1;

-- final table
update edx.dashboard_final_results set isCurrent = '0' where isCurrent = '1';

-- drop table if exists edx.dashboard_final_results;
-- create table edx.dashboard_final_results as
insert into edx.dashboard_final_results
select t.fiscal_month_dt,
       t.class,
       t.countryCode,
       t.countryCode_1,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.ace,
       d.fiscal_yr_and_per,
       d.fiscal_yr_and_per_desc,
       d.fiscal_yr_and_qtr_desc,
       d.current_month_flag,
       d.current_quarter_flag,
       d.current_year_flag,
       d.last_month_flag,
       d.last_year_flag,
       d.last_quarter_flag,
       d.same_quarter_last_year_flag,
       d.past_quarters,
       d.past_months,
       d.running_current_month_flag,
       d.running_current_quarter_flag,
       ifnull(t1.unique_acquisition, 0) as unique_acquisition,
       ifnull(t2.courses_enrolled, 0) + ifnull(t3.resource_downloaded, 0) as total_acquisition,
       ifnull(t2.courses_enrolled, 0) as courses_enrolled,
       ifnull(t3.resource_downloaded, 0) as resource_downloaded,
       ifnull(t4.new_user_content_acquisition, 0) as new_user_content_acquisition,
       ifnull(t5.ret_user_content_acquisition, 0) as ret_user_content_acquisition,
       ifnull(t6.unique_member_enrollment, 0) as unique_member_enrollment,
       ifnull(t7.new_unique_member_enrollment, 0) as new_unique_member_enrollment,
       ifnull(t8.unique_member_downloads, 0) as unique_member_downloads,
       ifnull(t9.new_unique_member_downloads, 0) as new_unique_member_downloads,
       ifnull(t10.new_member_courses_enrolled, 0) as new_member_courses_enrolled,
       ifnull(t11.ret_member_courses_enrolled, 0) as ret_member_courses_enrolled,
       ifnull(t12.new_member_resource_downloads, 0) as new_member_resource_downloads,
       ifnull(t13.ret_member_resource_downloads, 0) as ret_member_resource_downloads,
       ifnull(t6.unique_member_enrollment, 0) - ifnull(t7.new_unique_member_enrollment, 0) as ret_unique_member_enrollment,
       ifnull(t8.unique_member_downloads, 0) - ifnull(t9.new_unique_member_downloads, 0) as ret_unique_member_downloads,
       --
       ifnull(t14.mau_members, 0) + ifnull(t17.mau_visitors, 0) as mau_total_members,
       ifnull(t15.mau_new_members, 0) + ifnull(t18.mau_new_visitors, 0) as mau_total_new_members,
       ifnull(t14.mau_members, 0) + ifnull(t17.mau_visitors, 0) - ifnull(t15.mau_new_members, 0) - ifnull(t18.mau_new_visitors, 0) as mau_total_returning_members,
       --
       ifnull(t14.mau_members, 0) as mau_members,
       ifnull(t15.mau_new_members, 0) as mau_new_members,
       ifnull(t16.mau_returning_members, 0) as mau_returning_members,
       ifnull(t17.mau_visitors, 0) as mau_visitors,
       ifnull(t18.mau_new_visitors, 0) as mau_new_visitors,
       ifnull(t19.mau_returning_visitors, 0) as mau_returning_visitors,
       ifnull(t20.edex_new_user_signups, 0) as edex_new_user_signups,
       @population as population,
       current_timestamp() as population_dttm,
       1 as isCurrent
from ra.tmp_dashboard_final_base t
left join ra.tmp_dashboard_unique_acquisition t1 on t1.fiscal_month_dt = t.fiscal_month_dt
                                                and t1.class = t.class
                                                and t1.countryCode = t.countryCode
                                                and t1.countryCode_1 = t.countryCode_1
                                                and t1.sky_scraper = t.sky_scraper
                                                and t1.top_500 = t.top_500
                                                and t1.top_200 = t.top_200
                                                and t1.top_100 = t.top_100
                                                and t1.pod = t.pod
                                                and t1.ace = t.ace
                                                and t1.member_flag = t.member_flag
left join ra.tmp_dashboard_courses_enrolled t2 on t2.fiscal_month_dt = t.fiscal_month_dt
                                              and t2.class = t.class
                                              and t2.countryCode = t.countryCode
                                              and t2.countryCode_1 = t.countryCode_1
                                              and t2.sky_scraper = t.sky_scraper
                                              and t2.top_500 = t.top_500
                                              and t2.top_200 = t.top_200
                                              and t2.top_100 = t.top_100
                                              and t2.pod = t.pod
                                              and t2.ace = t.ace
                                              and t2.member_flag = t.member_flag
left join ra.tmp_dashboard_resource_downloaded t3 on t3.fiscal_month_dt = t.fiscal_month_dt
                                                      and t3.class = t.class
                                                      and t3.countryCode = t.countryCode
                                                      and t3.countryCode_1 = t.countryCode_1
                                                      and t3.sky_scraper = t.sky_scraper
                                                      and t3.top_500 = t.top_500
                                                      and t3.top_200 = t.top_200
                                                      and t3.top_100 = t.top_100
                                                      and t3.pod = t.pod
                                                      and t3.ace = t.ace
                                                      and t3.member_flag = t.member_flag
left join ra.tmp_dashboard_new_user_content_acquisition t4 on t4.fiscal_month_dt = t.fiscal_month_dt
                                                               and t4.class = t.class
                                                               and t4.countryCode = t.countryCode
                                                               and t4.countryCode_1 = t.countryCode_1
                                                               and t4.sky_scraper = t.sky_scraper
                                                               and t4.top_500 = t.top_500
                                                               and t4.top_200 = t.top_200
                                                               and t4.top_100 = t.top_100
                                                               and t4.pod = t.pod
                                                               and t4.ace = t.ace
                                                               and t4.member_flag = t.member_flag
left join ra.tmp_dashboard_ret_user_content_acquisition t5 on t5.fiscal_month_dt = t.fiscal_month_dt
                                                               and t5.class = t.class
                                                               and t5.countryCode = t.countryCode
                                                               and t5.countryCode_1 = t.countryCode_1
                                                               and t5.sky_scraper = t.sky_scraper
                                                               and t5.top_500 = t.top_500
                                                               and t5.top_200 = t.top_200
                                                               and t5.top_100 = t.top_100
                                                               and t5.pod = t.pod
                                                               and t5.ace = t.ace
                                                               and t5.member_flag = t.member_flag
left join ra.tmp_dashboard_unique_member_enrollment t6 on t6.fiscal_month_dt = t.fiscal_month_dt
                                                           and t6.class = t.class
                                                           and t6.countryCode = t.countryCode
                                                           and t6.countryCode_1 = t.countryCode_1
                                                           and t6.sky_scraper = t.sky_scraper
                                                           and t6.top_500 = t.top_500
                                                           and t6.top_200 = t.top_200
                                                           and t6.top_100 = t.top_100
                                                           and t6.pod = t.pod
                                                           and t6.ace = t.ace
                                                           and t6.member_flag = t.member_flag
left join ra.tmp_dashboard_new_unique_member_enrollment t7 on t7.fiscal_month_dt = t.fiscal_month_dt
                                                               and t7.class = t.class
                                                               and t7.countryCode = t.countryCode
                                                               and t7.countryCode_1 = t.countryCode_1
                                                               and t7.sky_scraper = t.sky_scraper
                                                               and t7.top_500 = t.top_500
                                                               and t7.top_200 = t.top_200
                                                               and t7.top_100 = t.top_100
                                                               and t7.pod = t.pod
                                                               and t7.ace = t.ace
                                                               and t7.member_flag = t.member_flag
left join ra.tmp_dashboard_unique_member_downloads t8 on t8.fiscal_month_dt = t.fiscal_month_dt
                                                          and t8.class = t.class
                                                          and t8.countryCode = t.countryCode
                                                          and t8.countryCode_1 = t.countryCode_1
                                                          and t8.sky_scraper = t.sky_scraper
                                                          and t8.top_500 = t.top_500
                                                          and t8.top_200 = t.top_200
                                                          and t8.top_100 = t.top_100
                                                          and t8.pod = t.pod
                                                          and t8.ace = t.ace
                                                          and t8.member_flag = t.member_flag
left join ra.tmp_dashboard_new_unique_member_downloads t9 on t9.fiscal_month_dt = t.fiscal_month_dt
                                                              and t9.class = t.class
                                                              and t9.countryCode = t.countryCode
                                                              and t9.countryCode_1 = t.countryCode_1
                                                              and t9.sky_scraper = t.sky_scraper
                                                              and t9.top_500 = t.top_500
                                                              and t9.top_200 = t.top_200
                                                              and t9.top_100 = t.top_100
                                                              and t9.pod = t.pod
                                                              and t9.ace = t.ace
                                                              and t9.member_flag = t.member_flag
left join ra.tmp_dashboard_new_member_courses_enrolled t10 on t10.fiscal_month_dt = t.fiscal_month_dt
                                                               and t10.class = t.class
                                                               and t10.countryCode = t.countryCode
                                                               and t10.countryCode_1 = t.countryCode_1
                                                               and t10.sky_scraper = t.sky_scraper
                                                               and t10.top_500 = t.top_500
                                                               and t10.top_200 = t.top_200
                                                               and t10.top_100 = t.top_100
                                                               and t10.pod = t.pod
                                                               and t10.ace = t.ace
                                                               and t10.member_flag = t.member_flag
left join ra.tmp_dashboard_ret_member_courses_enrolled t11 on t11.fiscal_month_dt = t.fiscal_month_dt
                                                               and t11.class = t.class
                                                               and t11.countryCode = t.countryCode
                                                               and t11.countryCode_1 = t.countryCode_1
                                                               and t11.sky_scraper = t.sky_scraper
                                                               and t11.top_500 = t.top_500
                                                               and t11.top_200 = t.top_200
                                                               and t11.top_100 = t.top_100
                                                               and t11.pod = t.pod
                                                               and t11.ace = t.ace
                                                               and t11.member_flag = t.member_flag
left join ra.tmp_dashboard_new_member_resource_downloads t12 on t12.fiscal_month_dt = t.fiscal_month_dt
                                                                 and t12.class = t.class
                                                                 and t12.countryCode = t.countryCode
                                                                 and t12.countryCode_1 = t.countryCode_1
                                                                 and t12.sky_scraper = t.sky_scraper
                                                                 and t12.top_500 = t.top_500
                                                                 and t12.top_200 = t.top_200
                                                                 and t12.top_100 = t.top_100
                                                                 and t12.pod = t.pod
                                                                 and t12.ace = t.ace
                                                                 and t12.member_flag = t.member_flag
left join ra.tmp_dashboard_ret_member_resource_downloads t13 on t13.fiscal_month_dt = t.fiscal_month_dt
                                                                 and t13.class = t.class
                                                                 and t13.countryCode = t.countryCode
                                                                 and t13.countryCode_1 = t.countryCode_1
                                                                 and t13.sky_scraper = t.sky_scraper
                                                                 and t13.top_500 = t.top_500
                                                                 and t13.top_200 = t.top_200
                                                                 and t13.top_100 = t.top_100
                                                                 and t13.pod = t.pod
                                                                 and t13.ace = t.ace
                                                                 and t13.member_flag = t.member_flag
left join ra.tmp_dashboard_mau_members t14 on t14.fiscal_month_dt = t.fiscal_month_dt
                                          and t14.class = t.class
                                          and t14.countryCode = t.countryCode
                                          and t14.countryCode_1 = t.countryCode_1
                                          and t14.sky_scraper = t.sky_scraper
                                          and t14.top_500 = t.top_500
                                          and t14.top_200 = t.top_200
                                          and t14.top_100 = t.top_100
                                          and t14.pod = t.pod
                                          and t14.ace = t.ace
                                          and t14.member_flag = t.member_flag
left join ra.tmp_dashboard_mau_new_members t15 on t15.fiscal_month_dt = t.fiscal_month_dt
                                              and t15.class = t.class
                                              and t15.countryCode = t.countryCode
                                              and t15.countryCode_1 = t.countryCode_1
                                              and t15.sky_scraper = t.sky_scraper
                                              and t15.top_500 = t.top_500
                                              and t15.top_200 = t.top_200
                                              and t15.top_100 = t.top_100
                                              and t15.pod = t.pod
                                              and t15.ace = t.ace
                                              and t15.member_flag = t.member_flag
left join ra.tmp_dashboard_mau_returning_members t16 on t16.fiscal_month_dt = t.fiscal_month_dt
                                                    and t16.class = t.class
                                                    and t16.countryCode = t.countryCode
                                                    and t16.countryCode_1 = t.countryCode_1
                                                    and t16.sky_scraper = t.sky_scraper
                                                    and t16.top_500 = t.top_500
                                                    and t16.top_200 = t.top_200
                                                    and t16.top_100 = t.top_100
                                                    and t16.pod = t.pod
                                                    and t16.ace = t.ace
                                                    and t16.member_flag = t.member_flag
left join ra.tmp_dashboard_mau_visitors t17 on t17.fiscal_month_dt = t.fiscal_month_dt
                                           and t17.class = t.class
                                           and t17.countryCode = t.countryCode
                                           and t17.countryCode_1 = t.countryCode_1
                                           and t17.sky_scraper = t.sky_scraper
                                           and t17.top_500 = t.top_500
                                           and t17.top_200 = t.top_200
                                           and t17.top_100 = t.top_100
                                           and t17.pod = t.pod
                                           and t17.ace = t.ace
                                           and t17.member_flag = t.member_flag
left join ra.tmp_dashboard_mau_new_visitors t18 on t18.fiscal_month_dt = t.fiscal_month_dt
                                               and t18.class = t.class
                                               and t18.countryCode = t.countryCode
                                               and t18.countryCode_1 = t.countryCode_1
                                               and t18.sky_scraper = t.sky_scraper
                                               and t18.top_500 = t.top_500
                                               and t18.top_200 = t.top_200
                                               and t18.top_100 = t.top_100
                                               and t18.pod = t.pod
                                               and t18.ace = t.ace
                                               and t18.member_flag = t.member_flag
left join ra.tmp_dashboard_mau_returning_visitors t19 on t19.fiscal_month_dt = t.fiscal_month_dt
                                                     and t19.class = t.class
                                                     and t19.countryCode = t.countryCode
                                                     and t19.countryCode_1 = t.countryCode_1
                                                     and t19.sky_scraper = t.sky_scraper
                                                     and t19.top_500 = t.top_500
                                                     and t19.top_200 = t.top_200
                                                     and t19.top_100 = t.top_100
                                                     and t19.pod = t.pod
                                                     and t19.ace = t.ace
                                                     and t19.member_flag = t.member_flag
left join ra.tmp_dashboard_edex_new_user_signups t20 on t20.fiscal_month_dt = t.fiscal_month_dt
                                                     and t20.class = t.class
                                                     and t20.countryCode = t.countryCode
                                                     and t20.countryCode_1 = t.countryCode_1
                                                     and t20.sky_scraper = t.sky_scraper
                                                     and t20.top_500 = t.top_500
                                                     and t20.top_200 = t.top_200
                                                     and t20.top_100 = t.top_100
                                                     and t20.pod = t.pod
                                                     and t20.ace = t.ace
                                                     and t20.member_flag = t.member_flag
left join ra.tmp_dashboard_adobe_fy_dates d on d.fiscal_month_dt = t.fiscal_month_dt
order by 1, 2, 3, 4, 5, 6, 7;

-- 
UPDATE edx.dashboard_final_results a
INNER JOIN ra.tmp_dashboard_adobe_fy_dates dd ON dd.fiscal_yr_and_per = a.fiscal_yr_and_per
SET a.fiscal_yr_and_per_desc = dd.fiscal_yr_and_per_desc,
    a.fiscal_yr_and_qtr_desc = dd.fiscal_yr_and_qtr_desc,
    a.current_month_flag = dd.current_month_flag,
    a.current_quarter_flag = dd.current_quarter_flag,
    a.current_year_flag = dd.current_year_flag,
    a.last_month_flag = dd.last_month_flag,
    a.last_year_flag = dd.last_year_flag,
    a.last_quarter_flag = dd.last_quarter_flag,
    a.past_quarters = dd.past_quarters,
    a.past_months = dd.past_months,
    a.running_current_month_flag = dd.running_current_month_flag,
    a.running_current_quarter_flag = dd.running_current_quarter_flag;
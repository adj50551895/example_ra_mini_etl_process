-- May contain fiscal months gap (@current_fiscal_yr_and_per)
-- If the date of the last start of the report and the current date of the start of the report are in a different fiscal month, it is necessary to start the report twice - for the last two fiscal months.

set @start_date_finer = '2021-12-04';
set @start_date = '2020-11-28';
set @fiscal_yr_and_per = 202012;

-- ----------------------------------------------------------------------------------------------------------------- EdEx Activity by District
-- EdEx Members
drop table if exists ra.tmp_district_edex_members;
create table ra.tmp_district_edex_members as
select d.district_id,
       d.school_district_name,
       d.domain,
       dd.fiscal_yr_and_per,
       case when d.sky_scraper = 0 then 0
            when d.sky_scraper is null then 0
            else 1
       end as sky_scraper,
       case when d.top_500 = '0' then 0
            when d.top_500 is null then 0
            else 1
       end as top_500,
       case when d.top_200 = '0' then 0
            when d.top_200 is null then 0
            else 1
       end as top_200,
       case when d.top_100 = '0' then 0
            when d.top_100 is null then 0
            else 1
       end as top_100,
       case when d.pod = '0' then 0
            when d.pod is null then 0
            else d.pod
       end as pod,
       -- d.nces_id,
       m.id as memberId,
       mp.userGuid,
       date(m.createdAt) as memberCreatedAt
from edx.Member m
join edx.district_domains d on d.domain COLLATE utf8mb4_general_ci = trim(lower(substring(m.email, position('@' in m.email)+1, length(m.email))))
left join hdp.memberguidmap mp on mp.memberId = m.id
join edx.hana_dim_date dd on dd.calendar_date = date(m.createdAt);


-- EdEx Members, group by, kolona B
drop temporary table if exists ra.tmp_district_edex_members_cnt;
create temporary table ra.tmp_district_edex_members_cnt as
select t.district_id,
       t.school_district_name,
       count(distinct t.memberId) as edex_members
from ra.tmp_district_edex_members t
group by t.district_id,
         t.school_district_name;


-- Members that Acquired Content, Kolona C
drop temporary table if exists ra.tmp_district_members_acquired_content;
create temporary table ra.tmp_district_members_acquired_content as
select t.district_id,
       t.school_district_name,
       count(distinct t.memberId) as members_acquired_content
from ra.tmp_district_edex_members t
where exists (select 1 from els.stg_events_acquisition_content aq where aq.memberId = t.memberId and aq.fiscal_yr_and_per > @fiscal_yr_and_per)
group by t.district_id,
         t.school_district_name;


-- edex_cc_usage, acrobat
drop temporary table if exists ra.tmp_edex_cc_usage_acrobat;
create temporary table ra.tmp_edex_cc_usage_acrobat as
select t.user_guid,
       min(t.min_activity_date) as first_activity_date
from hdp.ccmusg_fact_user_activity_cc_dc t
where t.product_category = 'DC'
group by t.user_guid;


-- edex_cc_usage, non acrobat
drop temporary table if exists ra.tmp_edex_cc_usage_non_acrobat;
create temporary table ra.tmp_edex_cc_usage_non_acrobat as
select t.user_guid,
       min(t.min_activity_date) as first_activity_date
from hdp.ccmusg_fact_user_activity_cc_dc t
where t.product_category = 'CC'
group by t.user_guid;


-- edex_spark_usage, first_activity_date
drop temporary table if exists ra.tmp_spark_event_activity_b_first_activity_date;
create temporary table ra.tmp_spark_event_activity_b_first_activity_date as 
select s.original_guid,
       min(s.min_event_date) as first_activity_date
from hdp.spark_event_activity_b_logins s
where exists (select 1 from ra.tmp_district_edex_members m where m.userGuid COLLATE utf8mb4_general_ci = s.original_guid)
group by s.original_guid;


drop table if exists ra.tmp_distrinct_product_first_usage;
create table ra.tmp_distrinct_product_first_usage as
select mp.memberId,
       mp.userGuid,
       min(case when coalesce(na.first_activity_date,'9999-01-01') = coalesce(s.first_activity_date,'9999-01-01') then 'Both' 
                when coalesce(na.first_activity_date,'9999-01-01') < coalesce(s.first_activity_date,'9999-01-01') then 'CC' 
                when coalesce(na.first_activity_date,'9999-01-01') > coalesce(s.first_activity_date,'9999-01-01') then 'Spark' end) as product,
	   min(coalesce(na.first_activity_date,'9999-01-01')) first_cc_use, 
       min(coalesce(aa.first_activity_date,'9999-01-01')) first_dc_use, 
       min(coalesce(s.first_activity_date,'9999-01-01')) first_spark_use, 
       min(case when coalesce(na.first_activity_date,'9999-01-01') <= coalesce(s.first_activity_date,'9999-01-01') then na.first_activity_date 
                when coalesce(aa.first_activity_date,'9999-01-01') <= coalesce(s.first_activity_date,'9999-01-01') then aa.first_activity_date 
		   else s.first_activity_date end ) first_use
from hdp.memberguidmap mp
left join ra.tmp_spark_event_activity_b_first_activity_date s on s.original_guid COLLATE utf8mb4_general_ci = mp.userGuid
left join ra.tmp_edex_cc_usage_acrobat aa on aa.user_guid COLLATE utf8mb4_general_ci = mp.userGuid
left join ra.tmp_edex_cc_usage_non_acrobat na on na.user_guid COLLATE utf8mb4_general_ci = mp.userGuid
group by mp.memberId,
         mp.userGuid;


drop temporary table if exists ra.tmp_district_edex_driven_first_time_logins;
create temporary table ra.tmp_district_edex_driven_first_time_logins as 
select m.district_id,
       m.school_district_name,
       count(distinct m.memberId) as edex_driven_first_time_logins,
       count(distinct m.memberId) * 16 as student_first_time_logins
from ra.tmp_distrinct_product_first_usage t
left join ra.tmp_district_edex_members m on m.memberId = t.memberId
where t.first_use >= @start_date
and t.first_use between m.memberCreatedAt and (m.memberCreatedAt + interval 30 day)
and exists (select 1 from els.stg_events_acquisition_content aq where aq.memberId = t.memberId and aq.fiscal_yr_and_per > @fiscal_yr_and_per)
group by m.district_id,
         m.school_district_name;
-- //--------------------------------------------------------------------------------------------------------------- EdEx Activity by District


drop temporary table if exists ra.tmp_district_activity_base;
create temporary table ra.tmp_district_activity_base as
select distinct t.district_id, 
                t.school_district_name
from ra.tmp_district_edex_members t;

-- update isCurrent
update edx.rpt_district_edex_activity_fin set isCurrent = '0' where isCurrent = '1';

-- drop table if exists edx.rpt_district_edex_activity_fin;
-- create table edx.rpt_district_edex_activity_fin as
insert into edx.rpt_district_edex_activity_fin
select b.district_id,
       b.school_district_name,
       ifnull(t1.edex_members, 0) as edex_members,
       ifnull(t2.members_acquired_content, 0) as members_acquired_content,
       ifnull((t2.members_acquired_content / t1.edex_members), 0) as acquisition_rate,
       ifnull(t3.edex_driven_first_time_logins, 0) as edex_driven_first_time_logins,
       ifnull(t3.student_first_time_logins, 0) as student_first_time_logins,
       ifnull((t3.edex_driven_first_time_logins / t2.members_acquired_content), 0) as first_time_login_rate,
	   current_timestamp() as createdAt,
       1 as isCurrent
from ra.tmp_district_activity_base b
left join ra.tmp_district_edex_members_cnt t1 on t1.district_id = b.district_id
left join ra.tmp_district_members_acquired_content t2 on t2.district_id = b.district_id
left join ra.tmp_district_edex_driven_first_time_logins t3 on t3.district_id = b.district_id
order by 2 desc;


-- ----------------------------------------------------------------------------------------------------------------- EdEx Activity Monthly
-- START

-- Base table
drop table if exists ra.tmp_district_base_members; -- ra.tmp_district_base_members
create table ra.tmp_district_base_members as
select t.district_id,
       t.school_district_name,
       t.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.memberid,
       t.userGuid,
       t.memberCreatedAt
from ra.tmp_district_edex_members t
where t.fiscal_yr_and_per > @fiscal_yr_and_per;
-- and t.school_district_name = 'Clark County School District';


drop table if exists ra.tmp_district_members_by_month;
create table ra.tmp_district_members_by_month as
select t.district_id,
       t.school_district_name,
       t.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       count(distinct t.memberid) as edex_members_by_month
from ra.tmp_district_edex_members t
where t.fiscal_yr_and_per > @fiscal_yr_and_per
-- and t.school_district_name = 'Clark County School District'
group by t.district_id,
         t.school_district_name,
         t.fiscal_yr_and_per,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod;


-- EdEx Visits by Month, kolona H
drop table if exists ra.tmp_district_visits_by_month;
create table ra.tmp_district_visits_by_month as
select t.district_id,
       t.school_district_name,
       dd.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       count(distinct e.memberId) as visits_by_month
from els.agg_elasticsearchevents_1 e
join ra.tmp_district_edex_members t on t.memberId = e.memberId
join edx.hana_dim_date dd on dd.calendar_date = e.event_date
where e.event_date >= @start_date_finer
-- and t.school_district_name = 'Clark County School District'
group by t.district_id,
         t.school_district_name,
         dd.fiscal_yr_and_per,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod;


drop temporary table if exists ra.tmp_district_member_first_time_content_acquisition;
create temporary table ra.tmp_district_member_first_time_content_acquisition
select min(t.fiscal_yr_and_per) as fiscal_yr_and_per,
       t.memberId
from els.stg_member_first_time_content_acquisition t
group by t.memberId;


/*
drop table if exists ra.tmp_district_base_member_acquisition;
create table ra.tmp_district_base_member_acquisition as
select t.district_id,
       t.school_district_name,
       dd.fiscal_yr_and_per,
       t.memberId,
       t.memberCreatedAt,
       t.sky_scraper,
       t.top_500,
       t.pod,
       stg.memberId as first_acquisition_ever_flag,
       count(*) as member_acquisition-- ,
       -- case when e.event_date <= @start_date then 1
       --      else 0
	   -- end as start_date
from els.events e -- els.agg_elasticsearchevents_1 e
join els.stg_acquisition_content_events acq on acq.event = e.event
join ra.tmp_district_edex_members t on t.memberId = e.memberId
join edx.hana_dim_date dd on dd.calendar_date = e.event_date
left join ra.tmp_district_member_first_time_content_acquisition stg on stg.fiscal_yr_and_per = dd.fiscal_yr_and_per
                                                                   and stg.memberId = t.memberId
where e.event_date >= @start_date
-- and t.school_district_name = 'Clark County School District'
group by t.district_id,
         t.school_district_name,
         dd.fiscal_yr_and_per,
         t.memberId,
         stg.memberId,
         t.memberCreatedAt,
         t.sky_scraper,
         t.top_500,
         t.pod;
*/
drop table if exists ra.tmp_district_base_member_acquisition;
create table ra.tmp_district_base_member_acquisition as
select t.district_id,
       t.school_district_name,
       acq.fiscal_yr_and_per,
       t.memberId,
       t.memberCreatedAt,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       stg.memberId as first_acquisition_ever_flag,
       count(*) as member_acquisition
from ra.tmp_district_edex_members t
join els.stg_events_acquisition_content acq on acq.memberId = t.memberId
left join ra.tmp_district_member_first_time_content_acquisition stg on stg.fiscal_yr_and_per = acq.fiscal_yr_and_per
                                                                   and stg.memberId = t.memberId
where acq.event_date >= @start_date
group by
t.district_id,
       t.school_district_name,
       acq.fiscal_yr_and_per,
       t.memberId,
       t.memberCreatedAt,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       stg.memberId;

-- Acquisition by Month, kolona L
-- First Time Acquisition by Month, kolona N
-- Repeat Acquisition by Month, kolona P
drop table if exists ra.tmp_district_member_acquisitions;
create table ra.tmp_district_member_acquisitions as
select t.district_id,
       t.school_district_name,
       t.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       sum(case when t.first_acquisition_ever_flag is not null then 1 else 0 end) as first_time_acquisition_by_month, -- first_acquisition,
       sum(case when (t.first_acquisition_ever_flag is null and t.member_acquisition >= 1) then 1 else 0 end) as repeat_acquisition_by_month, -- repeat_acquisition,
       sum(case when t.member_acquisition >= 1 then 1 else 0 end) as acquisition_by_month -- total_acquisition
from ra.tmp_district_base_member_acquisition t
where 1=1 -- start_date = 0
group by t.district_id,
         t.school_district_name,
         t.fiscal_yr_and_per,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod;


-- Acquisition by EdEx Members, kolona L, dodati kumuilativ J
drop temporary table if exists ra.tmp_district_acquisition_by_members;
create temporary table ra.tmp_district_acquisition_by_members as
select t.district_id,
       t.school_district_name,
       dd.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       count(distinct t.memberId) as acquisition_by_members
from ra.tmp_district_base_member_acquisition t
join edx.hana_dim_date dd on dd.calendar_date = date(t.memberCreatedAt)
where dd.fiscal_yr_and_per >= @fiscal_yr_and_per
group by t.district_id,
         t.school_district_name,
         dd.fiscal_yr_and_per,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod;


-- Express Correlation Logins by Month, kolona T
drop table if exists ra.tmp_district_express_correlation_logins;
create table ra.tmp_district_express_correlation_logins as
select t.district_id,
	   t.school_district_name,
       dd.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       count(distinct p.memberId) as express_correlation_logins
from ra.tmp_distrinct_product_first_usage p
join ra.tmp_district_edex_members t on t.memberId = p.memberId
join edx.hana_dim_date dd on dd.calendar_date = date(t.memberCreatedAt)
where p.first_spark_use != '9999-01-01'
-- and t.school_district_name = 'Clark County School District'
and dd.fiscal_yr_and_per >= @fiscal_yr_and_per
group by t.district_id,
         t.school_district_name,
         dd.fiscal_yr_and_per,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod;


-- Express Causation Logins by Month, kolona X
drop table if exists ra.tmp_district_express_causation_logins;
create table ra.tmp_district_express_causation_logins as
select t.district_id,
       t.school_district_name,
       dd.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       count(distinct p.memberId) as express_causation_logins
from ra.tmp_distrinct_product_first_usage p
join ra.tmp_district_edex_members t on t.memberId = p.memberId
join edx.hana_dim_date dd on dd.calendar_date = date(t.memberCreatedAt)
where p.first_spark_use != '9999-01-01'
and p.first_spark_use >= date(t.memberCreatedAt)
-- and t.school_district_name = 'Clark County School District'
and dd.fiscal_yr_and_per >= @fiscal_yr_and_per
group by t.district_id,
         t.school_district_name,
        dd.fiscal_yr_and_per,
        t.sky_scraper,
        t.top_500,
        t.top_200,
        t.top_100,
        t.pod;


-- CC Correlation Logins by Month, kolona AB
drop table if exists ra.tmp_district_cc_correlation_logins;
create table ra.tmp_district_cc_correlation_logins as
select t.district_id,
       t.school_district_name,
       dd.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       count(distinct p.memberId) as cc_correlation_logins
from ra.tmp_distrinct_product_first_usage p
join ra.tmp_district_edex_members t on t.memberId = p.memberId
join edx.hana_dim_date dd on dd.calendar_date = date(t.memberCreatedAt)
where p.first_cc_use != '9999-01-01'
-- and t.school_district_name = 'Clark County School District'
and dd.fiscal_yr_and_per >= @fiscal_yr_and_per
group by t.district_id,
         t.school_district_name,
         dd.fiscal_yr_and_per,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod;


-- CC Causation Logins by Month, kolona AF
drop table if exists ra.tmp_district_cc_causation_logins;
create table ra.tmp_district_cc_causation_logins as
select t.district_id,
       t.school_district_name,
       dd.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       count(distinct p.memberId) as cc_causation_logins
from ra.tmp_distrinct_product_first_usage p
join ra.tmp_district_edex_members t on t.memberId = p.memberId
join edx.hana_dim_date dd on dd.calendar_date = date(t.memberCreatedAt)
where p.first_cc_use != '9999-01-01'
-- and t.school_district_name = 'Clark County School District'
and p.first_cc_use >= date(t.memberCreatedAt)
and dd.fiscal_yr_and_per >= @fiscal_yr_and_per
group by t.district_id,
         t.school_district_name,
         dd.fiscal_yr_and_per,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod;
-- ----------------------------------------------------------------------------------------


drop table if exists ra.tmp_district_activity_monthly_base;
create table ra.tmp_district_activity_monthly_base as
select distinct t.district_id,
                t.school_district_name,
                t.fiscal_yr_and_per,
                t.sky_scraper,
                t.top_500,
                t.top_200,
                t.top_100,
                t.pod
from ra.tmp_district_base_members t
union
select distinct t.district_id,
                t.school_district_name,
                t.fiscal_yr_and_per,
                t.sky_scraper,
                t.top_500,
                t.top_200,
                t.top_100,
                t.pod
from ra.tmp_district_members_by_month t
union
select distinct t.district_id,
                t.school_district_name,
                t.fiscal_yr_and_per,
                t.sky_scraper,
                t.top_500,
                t.top_200,
                t.top_100,
                t.pod
from ra.tmp_district_visits_by_month t
union
select distinct t.district_id,
                t.school_district_name,
                t.fiscal_yr_and_per,
                t.sky_scraper,
                t.top_500,
                t.top_200,
                t.top_100,
                t.pod
from ra.tmp_district_member_acquisitions t
union
select distinct t.district_id,
                t.school_district_name,
                t.fiscal_yr_and_per,
                t.sky_scraper,
                t.top_500,
                t.top_200,
                t.top_100,
                t.pod
from ra.tmp_district_acquisition_by_members t
union
select distinct t.district_id,
                t.school_district_name,
                t.fiscal_yr_and_per,
                t.sky_scraper,
                t.top_500,
                t.top_200,
                t.top_100,
                t.pod
from ra.tmp_district_express_correlation_logins t
union
select distinct t.district_id,
                t.school_district_name,
                t.fiscal_yr_and_per,
                t.sky_scraper,
                t.top_500,
                t.top_200,
                t.top_100,
                t.pod
from ra.tmp_district_express_causation_logins t
union
select distinct t.district_id,
                t.school_district_name,
                t.fiscal_yr_and_per,
                t.sky_scraper,
                t.top_500,
                t.top_200,
                t.top_100,
                t.pod
from ra.tmp_district_cc_correlation_logins t
union
select distinct t.district_id,
                t.school_district_name,
                t.fiscal_yr_and_per,
                t.sky_scraper,
                t.top_500,
                t.top_200,
                t.top_100,
                t.pod
from ra.tmp_district_cc_causation_logins t;


-- final table
-- update edx.rpt_content_detail_course set isCurrent = '0' where isCurrent = '1';
drop table if exists ra.tmp_district_report_final_non_cumulative;
create table ra.tmp_district_report_final_non_cumulative as
select b.district_id,
       b.school_district_name,
       b.fiscal_yr_and_per,
       b.sky_scraper,
       b.top_500,
       b.top_200,
       b.top_100,
       b.pod,
       -- b.nces_id,
       ifnull(t1.edex_members_by_month, 0) as edex_members_by_month, -- as "EdEx Members by Month",
       ifnull(t2.visits_by_month, 0) as visits_by_month,-- as "EdEx Visits by Month",
       ifnull(t3.first_time_acquisition_by_month, 0)  as first_time_acquisition_by_month,-- as "First Time Acquisition by Month",
       ifnull(t3.repeat_acquisition_by_month, 0)  as repeat_acquisition_by_month,-- as "Repeat Acquisition by Month",
       ifnull(t3.acquisition_by_month, 0)  as acquisition_by_month,-- as "Acquisition by Month",
       ifnull(t4.acquisition_by_members, 0)  as acquisition_by_members,-- as "Acquisition by EdEx Members",
       ifnull(t5.express_correlation_logins, 0)  as express_correlation_logins,-- as "Express Correlation Logins by Month",
       ifnull(t6.express_causation_logins, 0)  as express_causation_logins,-- as "Express Causation Logins by Month",
       ifnull(t7.cc_correlation_logins, 0)  as cc_correlation_logins,-- as "CC Correlation Logins by Month",
       ifnull(t8.cc_causation_logins, 0)  as cc_causation_logins -- as "CC Causation Logins by Month"
      -- t7.express_correlation_logins
from ra.tmp_district_activity_monthly_base b
left join ra.tmp_district_members_by_month t1 on t1.district_id = b.district_id
                                             and t1.fiscal_yr_and_per = b.fiscal_yr_and_per
                                             and t1.sky_scraper = b.sky_scraper
                                             and t1.top_500 = b.top_500
                                             and t1.top_200 = b.top_200
                                             and t1.top_100 = b.top_100
                                             and t1.pod = b.pod
left join ra.tmp_district_visits_by_month t2 on t2.district_id = b.district_id
                                            and t2.fiscal_yr_and_per = b.fiscal_yr_and_per
                                            and t2.sky_scraper = b.sky_scraper
                                            and t2.top_500 = b.top_500
                                            and t2.top_200 = b.top_200
                                            and t2.top_100 = b.top_100
                                            and t2.pod = b.pod
left join ra.tmp_district_member_acquisitions t3 on t3.district_id = b.district_id
                                                and t3.fiscal_yr_and_per = b.fiscal_yr_and_per
                                                and t3.sky_scraper = b.sky_scraper
                                                and t3.top_500 = b.top_500
                                                and t3.top_200 = b.top_200
                                                and t3.top_100 = b.top_100
                                                and t3.pod = b.pod
left join ra.tmp_district_acquisition_by_members t4 on t4.district_id = b.district_id
                                                   and t4.fiscal_yr_and_per = b.fiscal_yr_and_per
                                                   and t4.sky_scraper = b.sky_scraper
                                                   and t4.top_500 = b.top_500
                                                   and t4.top_200 = b.top_200
                                                   and t4.top_100 = b.top_100
                                                   and t4.pod = b.pod
left join ra.tmp_district_express_correlation_logins t5 on t5.district_id = b.district_id
                                                       and t5.fiscal_yr_and_per = b.fiscal_yr_and_per
                                                       and t5.sky_scraper = b.sky_scraper
                                                       and t5.top_500 = b.top_500
                                                       and t5.top_200 = b.top_200
                                                       and t5.top_100 = b.top_100
                                                       and t5.pod = b.pod
left join ra.tmp_district_express_causation_logins t6 on t6.district_id = b.district_id
                                                     and t6.fiscal_yr_and_per = b.fiscal_yr_and_per
                                                     and t6.sky_scraper = b.sky_scraper
                                                     and t6.top_500 = b.top_500
                                                     and t6.top_200 = b.top_200
                                                     and t6.top_100 = b.top_100
                                                     and t6.pod = b.pod
left join ra.tmp_district_cc_correlation_logins t7 on t7.district_id = b.district_id
                                                  and t7.fiscal_yr_and_per = b.fiscal_yr_and_per
                                                  and t7.sky_scraper = b.sky_scraper
                                                  and t7.top_500 = b.top_500
                                                  and t7.top_200 = b.top_200
                                                  and t7.top_100 = b.top_100
                                                  and t7.pod = b.pod
left join ra.tmp_district_cc_causation_logins t8 on t8.district_id = b.district_id
                                                and t8.fiscal_yr_and_per = b.fiscal_yr_and_per
                                                and t8.sky_scraper = b.sky_scraper
                                                and t8.top_500 = b.top_500
                                                and t8.top_200 = b.top_200
                                                and t8.top_100 = b.top_100
                                                and t8.pod = b.pod;


-- Calculating Cumulative columns

-- EdEx_Members, kolona F
-- EdEx Members by Month, kolona I
drop temporary table if exists ra.tmp_district_members_by_month_cum;
create temporary table ra.tmp_district_members_by_month_cum as
select t1.district_id,
       t1.school_district_name,
       t1.fiscal_yr_and_per,
       t1.sky_scraper,
       t1.top_500,
       t1.top_200,
       t1.top_100,
       t1.pod,
       sum(t2.edex_members_by_month) as edex_members_by_month_cum,
       t1.edex_members_by_month
from ra.tmp_district_report_final_non_cumulative t1
inner join ra.tmp_district_report_final_non_cumulative t2 on t1.fiscal_yr_and_per >= t2.fiscal_yr_and_per 
                                                         and t1.district_id = t2.district_id
group by t1.district_id,
         t1.school_district_name,
         t1.fiscal_yr_and_per,
         t1.sky_scraper,
         t1.top_500,
         t1.top_200,
         t1.top_100,
         t1.pod,
         t1.edex_members_by_month;


-- EdEx Overall Visits, kolona G
drop temporary table if exists ra.tmp_district_overall_visits_cum;
create temporary table ra.tmp_district_overall_visits_cum as
select t1.district_id,
       t1.school_district_name,
       t1.fiscal_yr_and_per,
       t1.sky_scraper,
       t1.top_500,
       t1.top_200,
       t1.top_100,
       t1.pod,
       sum(t2.visits_by_month) as overall_visits,
       t1.visits_by_month
from ra.tmp_district_report_final_non_cumulative t1
inner join ra.tmp_district_report_final_non_cumulative t2 on t1.fiscal_yr_and_per >= t2.fiscal_yr_and_per
                                                         and t1.district_id = t2.district_id
-- and t1.school_district_name = 'Clark County School District'
group by t1.district_id,
         t1.school_district_name,
         t1.fiscal_yr_and_per,
         t1.sky_scraper,
         t1.top_500,
         t1.top_200,
         t1.top_100,
         t1.pod,
         t1.visits_by_month;


-- Express Correlation Logins, kolona R
drop temporary table if exists ra.tmp_express_correlation_logins_cum;
create temporary table ra.tmp_express_correlation_logins_cum as
select t1.district_id,
       t1.school_district_name,
       t1.fiscal_yr_and_per,
       t1.sky_scraper,
       t1.top_500,
       t1.top_200,
       t1.top_100,
       t1.pod,
       sum(t2.express_correlation_logins) as cum_express_correlation_logins,
       t1.express_correlation_logins
from ra.tmp_district_report_final_non_cumulative t1
inner join ra.tmp_district_report_final_non_cumulative t2 on t1.fiscal_yr_and_per >= t2.fiscal_yr_and_per
                                                         and t1.district_id = t2.district_id
-- and t1.school_district_name = 'Clark County School District'
group by t1.district_id,
         t1.school_district_name,
         t1.fiscal_yr_and_per,
         t1.sky_scraper,
         t1.top_500,
         t1.top_200,
         t1.top_100,
         t1.pod,
         t1.express_correlation_logins;


-- Express Causation Logins, kolona V
drop temporary table if exists ra.tmp_express_causation_logins_cum;
create temporary table ra.tmp_express_causation_logins_cum as
select t1.district_id,
       t1.school_district_name,
       t1.fiscal_yr_and_per,
       t1.sky_scraper,
       t1.top_500,
       t1.top_200,
       t1.top_100,
       t1.pod,
       sum(t2.express_causation_logins) as cum_express_causation_logins,
       t1.express_causation_logins
from ra.tmp_district_report_final_non_cumulative t1
inner join ra.tmp_district_report_final_non_cumulative t2 on t1.fiscal_yr_and_per >= t2.fiscal_yr_and_per
                                                         and t1.district_id = t2.district_id
-- and t1.school_district_name = 'Clark County School District'
group by t1.district_id,
         t1.school_district_name,
         t1.fiscal_yr_and_per,
         t1.sky_scraper,
         t1.top_500,
         t1.top_200,
         t1.top_100,
         t1.pod,
         t1.express_causation_logins;


-- CC Correlation Logins, kolona Z
drop temporary table if exists ra.tmp_cc_correlation_logins_cum;
create temporary table ra.tmp_cc_correlation_logins_cum as
select t1.district_id,
       t1.school_district_name,
       t1.fiscal_yr_and_per,
       t1.sky_scraper,
       t1.top_500,
       t1.top_200,
       t1.top_100,
       t1.pod,
       sum(t2.cc_correlation_logins) as cum_cc_correlation_logins,
       t1.cc_correlation_logins
from ra.tmp_district_report_final_non_cumulative t1
inner join ra.tmp_district_report_final_non_cumulative t2 on t1.fiscal_yr_and_per >= t2.fiscal_yr_and_per
                                                         and t1.district_id = t2.district_id
-- and t1.school_district_name = 'Clark County School District'
group by t1.district_id,
         t1.school_district_name,
         t1.fiscal_yr_and_per,
         t1.sky_scraper,
         t1.top_500,
         t1.top_200,
         t1.top_100,
         t1.pod,
         t1.cc_correlation_logins;

-- CC Causation Logins, kolona AD
drop temporary table if exists ra.tmp_cc_causation_logins_cum;
create temporary table ra.tmp_cc_causation_logins_cum as
select t1.district_id,
       t1.school_district_name,
       t1.fiscal_yr_and_per,
       t1.sky_scraper,
       t1.top_500,
       t1.top_200,
       t1.top_100,
       t1.pod,
       sum(t2.cc_causation_logins) as cum_cc_causation_logins,
       t1.cc_causation_logins
from ra.tmp_district_report_final_non_cumulative t1
inner join ra.tmp_district_report_final_non_cumulative t2 on t1.fiscal_yr_and_per >= t2.fiscal_yr_and_per 
                                                         and t1.district_id = t2.district_id
-- and t1.school_district_name = 'Clark County School District'
group by t1.district_id,
         t1.school_district_name,
         t1.fiscal_yr_and_per,
         t1.sky_scraper,
         t1.top_500,
         t1.top_200,
         t1.top_100,
         t1.pod,
         t1.cc_causation_logins;

-- Acquisition by EdEx Members, kolona J
drop temporary table if exists ra.tmp_acquisition_by_members_cum;
create temporary table ra.tmp_acquisition_by_members_cum as
select t1.district_id,
       t1.school_district_name,
       t1.fiscal_yr_and_per,
       t1.sky_scraper,
       t1.top_500,
       t1.top_200,
       t1.top_100,
       t1.pod,
       sum(t2.acquisition_by_members) as cum_acquisition_by_members,
       t1.acquisition_by_members
from ra.tmp_district_report_final_non_cumulative t1
inner join ra.tmp_district_report_final_non_cumulative t2 on t1.fiscal_yr_and_per >= t2.fiscal_yr_and_per
                                                         and t1.district_id = t2.district_id
-- and t1.school_district_name = 'Clark County School District'
group by t1.district_id,
         t1.school_district_name,
         t1.fiscal_yr_and_per,
         t1.sky_scraper,
         t1.top_500,
         t1.top_200,
         t1.top_100,
         t1.pod,
         t1.acquisition_by_members;

-- final table
-- update isCurrent
update edx.rpt_district_edex_activity_monthly set isCurrent = '0' where isCurrent = '1';

-- drop table if exists edx.rpt_district_edex_activity_monthly;-- _final;
-- create table edx.rpt_district_edex_activity_monthly -- _final as
insert into edx.rpt_district_edex_activity_monthly
select distinct
       t.district_id,
       t.school_district_name,
       t.fiscal_yr_and_per,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       -- t.nces_id,
       t.edex_members_by_month,
       t1.edex_members_by_month_cum,
       t.visits_by_month,
       t2.overall_visits,
       t7.cum_acquisition_by_members,
       ifnull(t7.cum_acquisition_by_members / t1.edex_members_by_month_cum, 0) as rate_content_acquisition_by_members,
       t.acquisition_by_month,
       ifnull(t.acquisition_by_month / t.visits_by_month, 0) as rate_content_acquisition_by_month,
       t.first_time_acquisition_by_month,
       ifnull(t.first_time_acquisition_by_month / t.acquisition_by_month, 0) as rate_first_content_acquisition_by_members,
       t.repeat_acquisition_by_month,
       ifnull(t.repeat_acquisition_by_month / t.acquisition_by_month, 0) as rate_repeat_content_acquisition_by_members,
       t3.cum_express_correlation_logins,
       ifnull(t3.cum_express_correlation_logins / t1.edex_members_by_month_cum, 0) as rate_correlated_express_logins,
       t.express_correlation_logins,
       ifnull(t.express_correlation_logins / t.edex_members_by_month, 0) as rate_correlated_express_logins_monthly,
       t4.cum_express_causation_logins,
       ifnull(t4.cum_express_causation_logins / t1.edex_members_by_month_cum, 0) as rate_causation_express_logins,
       t.express_causation_logins,
       ifnull(t.express_causation_logins / t.edex_members_by_month, 0) as rate_causation_express_logins_monthly,
       t5.cum_cc_correlation_logins,
       ifnull(t5.cum_cc_correlation_logins / t1.edex_members_by_month_cum, 0) as rate_cc_correlation_logins,
       t.cc_correlation_logins,
       ifnull(t.cc_correlation_logins / t.edex_members_by_month, 0) as rate_cc_correlation_logins_monthly,
       t6.cum_cc_causation_logins,
       ifnull(t6.cum_cc_causation_logins / t1.edex_members_by_month_cum, 0) as rate_cc_causation_logins,
       t.cc_causation_logins,
       ifnull(t.cc_causation_logins / t.edex_members_by_month, 0) as rate_cc_causation_logins_monthly,
       current_timestamp() as createdAt,
       1 as isCurrent
from ra.tmp_district_report_final_non_cumulative t
left join ra.tmp_district_members_by_month_cum t1 on t1.district_id = t.district_id
                                                 and t1.fiscal_yr_and_per = t.fiscal_yr_and_per
                                                 and t1.sky_scraper = t.sky_scraper
                                                 and t1.top_500 = t.top_500
                                                 and t1.top_200 = t.top_200
                                                 and t1.top_100 = t.top_100
                                                 and t1.pod = t.pod
left join ra.tmp_district_overall_visits_cum t2 on t2.district_id = t.district_id
                                               and t2.fiscal_yr_and_per = t.fiscal_yr_and_per
                                               and t2.sky_scraper = t.sky_scraper
                                               and t2.top_500 = t.top_500
                                               and t2.top_200 = t.top_200
                                               and t2.top_100 = t.top_100
                                               and t2.pod = t.pod
left join ra.tmp_express_correlation_logins_cum t3 on t3.district_id = t.district_id
                                                  and t3.fiscal_yr_and_per = t.fiscal_yr_and_per
                                                  and t3.sky_scraper = t.sky_scraper
                                                  and t3.top_500 = t.top_500
                                                  and t3.top_200 = t.top_200
                                                  and t3.top_100 = t.top_100
                                                  and t3.pod = t.pod
left join ra.tmp_express_causation_logins_cum t4 on t4.district_id = t.district_id
                                                and t4.fiscal_yr_and_per = t.fiscal_yr_and_per
                                                and t4.sky_scraper = t.sky_scraper
                                                and t4.top_500 = t.top_500
                                                and t4.top_200 = t.top_200
                                                and t4.top_100 = t.top_100
                                                and t4.pod = t.pod
left join ra.tmp_cc_correlation_logins_cum t5 on t5.district_id = t.district_id
                                             and t5.fiscal_yr_and_per = t.fiscal_yr_and_per
                                             and t5.sky_scraper = t.sky_scraper
                                             and t5.top_500 = t.top_500
                                             and t5.top_200 = t.top_200
                                             and t5.top_100 = t.top_100
                                             and t5.pod = t.pod
left join ra.tmp_cc_causation_logins_cum t6 on t6.district_id = t.district_id
                                           and t6.fiscal_yr_and_per = t.fiscal_yr_and_per
                                           and t6.sky_scraper = t.sky_scraper
                                           and t6.top_500 = t.top_500
                                           and t6.top_200 = t.top_200
                                           and t6.top_100 = t.top_100
                                           and t6.pod = t.pod
left join ra.tmp_acquisition_by_members_cum t7 on t7.district_id = t.district_id
                                              and t7.fiscal_yr_and_per = t.fiscal_yr_and_per
                                              and t7.sky_scraper = t.sky_scraper
                                              and t7.top_500 = t.top_500
                                              and t7.top_200 = t.top_200
                                              and t7.top_100 = t.top_100
                                              and t7.pod = t.pod;

-- 01.09.2023 Issue with missing cumulative data when no new members were created or no activity at all from members belonging to one district
select DATE_FORMAT(DATE_SUB(date(concat(max(t.fiscal_yr_and_per),'01')), interval 2 month),'%Y%m'), max(t.fiscal_yr_and_per)
into @m_fiscal_yr_and_per_start, @m_fiscal_yr_and_per_end
from edx.rpt_district_edex_activity_monthly t;

-- cross join
drop temporary table if exists ra.tmp_fiscal_yr_and_per_district;
create temporary table ra.tmp_fiscal_yr_and_per_district as
SELECT distinct d.fiscal_yr_and_per,
                t.district_id
FROM edx.hana_dim_date d
CROSS JOIN edx.rpt_district_edex_activity_monthly t
where d.fiscal_yr_and_per between @m_fiscal_yr_and_per_start and @m_fiscal_yr_and_per_end
and t.isCurrent = 1;

-- identify missing rows for insert
drop table if exists ra.tmp_fiscal_yr_and_per_district_to_insert;
create table ra.tmp_fiscal_yr_and_per_district_to_insert as
select distinct d.fiscal_yr_and_per,
                d.district_id,
                (select max(b1.fiscal_yr_and_per) from edx.rpt_district_edex_activity_monthly b1 where b1.district_id = d.district_id and b1.fiscal_yr_and_per < d.fiscal_yr_and_per and b1.isCurrent = 1) as prev_fiscal_yr_and_per
from ra.tmp_fiscal_yr_and_per_district d
left join edx.rpt_district_edex_activity_monthly t on t.fiscal_yr_and_per = d.fiscal_yr_and_per
                                                  and t.district_id = d.district_id
where t.isCurrent = 1
and t.district_id is null
and d.fiscal_yr_and_per >= (select min(t1.fiscal_yr_and_per) from edx.rpt_district_edex_activity_monthly t1 where t1.district_id = d.district_id);

-- insert missing vlaues to the monthly table
insert into edx.rpt_district_edex_activity_monthly
select  t.district_id,
        t.school_district_name, 
        s.fiscal_yr_and_per,
        t.sky_scraper, 
        t.top_500, 
        t.top_200, 
        t.top_100, 
        t.pod, 
        0 as edex_members_by_month, 
        t.edex_members_by_month_cum, 
        0 as visits_by_month, 
        0 as overall_visits, 
        t.cum_acquisition_by_members, 
        t.rate_content_acquisition_by_members, 
        0 as acquisition_by_month, 
        0 as rate_content_acquisition_by_month, 
        0 as first_time_acquisition_by_month, 
        0 as rate_first_content_acquisition_by_members, 
        0 as repeat_acquisition_by_month, 
        0 as rate_repeat_content_acquisition_by_members, 
        t.cum_express_correlation_logins, 
        t.rate_correlated_express_logins, 
        0 as express_correlation_logins, 
        0 as rate_correlated_express_logins_monthly, 
        t.cum_express_causation_logins, 
        t.rate_causation_express_logins, 
        0 as express_causation_logins, 
        0 as rate_causation_express_logins_monthly, 
        t.cum_cc_correlation_logins, 
        t.rate_cc_correlation_logins, 
        0 as cc_correlation_logins, 
        0 as rate_cc_correlation_logins_monthly, 
        t.cum_cc_causation_logins, 
        t.rate_cc_causation_logins, 
        0 as cc_causation_logins, 
        0 as rate_cc_causation_logins_monthly, 
        t.createdAt, 
        1 as isCurrent
from ra.tmp_fiscal_yr_and_per_district_to_insert s
join edx.rpt_district_edex_activity_monthly t on t.fiscal_yr_and_per = s.prev_fiscal_yr_and_per
                                             and t.district_id = s.district_id
                                             and t.isCurrent = 1;
-- //01.09.2023 Issue with missing cumulative data when no new members were created or no activity at all from members belonging to one district

-- 28.04.2023 because of the history data, fiscal month gap
-- current month
select dd.fiscal_yr_and_per
into @current_fiscal_yr_and_per
from edx.hana_dim_date dd 
where dd.calendar_date = current_date();

-- prev month
select max(fiscal_yr_and_per)
into @report_fiscal_yr_and_per
from edx.rpt_district_edex_activity_monthly_fin;

select if(@current_fiscal_yr_and_per != @report_fiscal_yr_and_per, 1, 0)
into @prev_month;

-- set @current_fiscal_yr_and_per = 202305;
-- set @prev_month = 0;

-- prev month
delete from edx.rpt_district_edex_activity_monthly_fin where fiscal_yr_and_per = @report_fiscal_yr_and_per and 1=@prev_month;

insert into edx.rpt_district_edex_activity_monthly_fin
select *
from edx.rpt_district_edex_activity_monthly t
where t.isCurrent = 1
and t.fiscal_yr_and_per = @report_fiscal_yr_and_per
and 1=@prev_month;

-- current month
delete from edx.rpt_district_edex_activity_monthly_fin where fiscal_yr_and_per = @current_fiscal_yr_and_per;

insert into edx.rpt_district_edex_activity_monthly_fin
select *
from edx.rpt_district_edex_activity_monthly t
where t.isCurrent = 1
and t.fiscal_yr_and_per = @current_fiscal_yr_and_per;
-- //28.04.2023 because of the history data
-- base # K12 US members
drop table if exists ra.tmp_k12_members;
create table ra.tmp_k12_members as
select m.id as memberId,
       mp.userGuid
from edx.Member m
join hdp.memberguidmap mp on mp.memberId = m.id
join edx.membersegmentation s on s.memberId = m.id
left join edx.ud_lookup l on l.memberID = m.id and l.countryCode = 'US'
where 1=1
and s.class = 'K12'
and (m.countryCode  = 'US' or (m.countryCode = 'UD' and l.countryCode = 'US'));

-- base # of US K12 EdEx (logged in) users in this month
drop table if exists ra.tmp_member_k12_loggedIn;
create table ra.tmp_member_k12_loggedIn as
select e.memberId,
       e.event,
       e.eventLevel,
       e.entityType,
       dd.fiscal_yr_and_per_desc,
       dd.fiscal_yr_and_per,
       min(e.event_date) min_event_date,
       max(e.event_date) as max_event_date,
       count(distinct e.memberId) as cn_
from els.agg_elasticsearchevents_1 e
join edx.hana_dim_date dd on dd.calendar_date = e.event_date
where e.event_date > '2020-11-27'
and e.memberId is not null
group by e.memberId, e.event, e.eventLevel, e.entityType, dd.fiscal_yr_and_per_desc, dd.fiscal_yr_and_per;

-- base # Acquired Content
drop table if exists ra.tmp_member_K12_acq_content;
create table ra.tmp_member_K12_acq_content as
select e.memberId,
       -- e.event,
       dd.fiscal_yr_and_per_desc,
       min(e.event_date) min_event_date,
       max(e.event_date) as max_event_date,
       count(*) as count_
from els.agg_elasticsearchevents_1 e
join edx.hana_dim_date dd on dd.calendar_date = e.event_date
where e.event in (select e1.event from edx.content_acquisition_events e1)
and e.event_date > '2019-11-29'
and e.memberId is not null
group by e.memberId, dd.fiscal_yr_and_per_desc;

-- Sheet 1, B # of US K12 EdEx logged in members in total
drop table if exists ra.tmp_US_K12_LoggendIn;
create temporary table ra.tmp_US_K12_LoggendIn as
select t.fiscal_yr_and_per_desc,
	   count(distinct m.memberId) cnt_distinct_members
from ra.tmp_member_k12_loggedIn t
join ra.tmp_k12_members m on m.memberId = t.memberId
group by t.fiscal_yr_and_per_desc;

-- Sheet 1, C # that acquired content any time in last 12 mo
drop table if exists ra.tmp_US_K12_AcquiredContent;
create temporary table ra.tmp_US_K12_AcquiredContent as
select b.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as cnt_distinct_members
from ra.tmp_k12_members t
join ra.tmp_member_k12_loggedIn b on b.memberId =  t.memberId
where exists (
select 1
from ra.tmp_member_K12_acq_content a
where 1=1
and a.memberId = t.memberId
and a.max_event_date >= b.min_event_date - interval 12 month
and a.max_event_date < b.min_event_date --
)
group by b.fiscal_yr_and_per_desc;


-- Sheet 1, D # that visited Express any time in last 12 mo
drop table if exists ra.tmp_US_K12_VisitedExpress;
create temporary table ra.tmp_US_K12_VisitedExpress as
select b.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as cnt_distinct_members
from ra.tmp_k12_members t
join ra.tmp_member_k12_loggedIn b on b.memberId =  t.memberId
where exists (
select 1
from hdp.mcietl_web_visits_detailed_express_clicks e
where 1=1
and e.guid = t.userGuid COLLATE utf8mb4_general_ci
and e.last_expresss_time >= b.min_event_date - interval 12 month
and e.last_expresss_time < b.min_event_date --
)
group by b.fiscal_yr_and_per_desc;


-- Sheet 1, E # that acquired content AND visited Express in last 12 mo
drop table if exists ra.tmp_US_K12_AcquiredContentAndVisitedExpress;
create temporary table ra.tmp_US_K12_AcquiredContentAndVisitedExpress as
select b.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as cnt_distinct_members
from ra.tmp_k12_members t
join ra.tmp_member_k12_loggedIn b on b.memberId =  t.memberId
where 1=1
and exists (
select 1
from ra.tmp_member_K12_acq_content a
where 1=1
and a.memberId = t.memberId
and a.max_event_date >= b.min_event_date - interval 12 month
and a.max_event_date < b.min_event_date --
)
and exists (
select 1
from hdp.mcietl_web_visits_detailed_express_clicks e
where 1=1
and e.guid = t.userGuid COLLATE utf8mb4_general_ci
and e.last_expresss_time >= b.min_event_date - interval 12 month
and e.last_expresss_time < b.min_event_date --
)
group by b.fiscal_yr_and_per_desc;


-- final
-- # Sheet 12 month rolling view - summary
drop table if exists edx.rpt_conversion_benchmark_1;
create table edx.rpt_conversion_benchmark_1 as
select l.fiscal_yr_and_per_desc as "fiscal_month_year",
       l.cnt_distinct_members as "K12_edex_logged_members_total",
       v.cnt_distinct_members as "acquired_content_in_last_12_mo",
       a.cnt_distinct_members as "visited_express_in_last_12_mo",
       av.cnt_distinct_members as "acquired_content_visited_express_in_last_12_mo",
       av.cnt_distinct_members/l.cnt_distinct_members as "acquired_content_visited_express_in_last_12_mo_perc"
from ra.tmp_US_K12_LoggendIn l
join ra.tmp_US_K12_AcquiredContent a on a.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
join ra.tmp_US_K12_VisitedExpress v on v.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
join ra.tmp_US_K12_AcquiredContentAndVisitedExpress av on av.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc;


-- drop table ra.tmp_k12_members;
-- drop table ra.tmp_member_k12_loggedIn;
-- drop table ra.tmp_member_K12_acq_content;
-- drop table ra.tmp_US_K12_LoggendIn;
drop table ra.tmp_US_K12_AcquiredContent;
drop table ra.tmp_US_K12_VisitedExpress;
drop table ra.tmp_US_K12_AcquiredContentAndVisitedExpress;
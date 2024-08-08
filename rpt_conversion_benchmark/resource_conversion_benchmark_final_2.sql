-- base Sheet 2, tmp_open_tp_visit_challenges
drop table if exists ra.tmp_open_tp_visit_challenges;
create table ra.tmp_open_tp_visit_challenges as
select t.memberId,
       t.userGuid,
       b.fiscal_yr_and_per_desc,
       b.eventLevel as event_page_url
from ra.tmp_k12_members t
join ra.tmp_member_k12_loggedIn b on b.memberId =  t.memberId
where 1=1
and b.eventLevel in ('view', 'download')
and b.entityType = 'resource'
group by t.memberId, t.userGuid, b.fiscal_yr_and_per_desc, b.eventLevel
union all
select t.memberId,
       t.userGuid,
       b.fiscal_yr_and_per_desc,
       case when lower(e.page_url) like '%edex.adobe.com/challenges%' then 'challenge'
            when lower(e.page_url) like '%cdn.edex.adobe.com%' then 'instruction_card'
       end as event_page_url
from ra.tmp_k12_members t
join ra.tmp_member_k12_loggedIn b on b.memberId =  t.memberId
join hdp.mcietl_web_visits_detailed_edex_clicks e on e.guid = t.userGuid COLLATE utf8mb4_general_ci
 join edx.hana_dim_date dd on dd.calendar_date = e.click_date
           
           and dd.fiscal_yr_and_per_desc = b.fiscal_yr_and_per_desc
where 1=1
and (lower(e.page_url) like '%edex.adobe.com/challenges%' or lower(e.page_url) like '%cdn.edex.adobe.com%')
group by t.memberId, t.userGuid, b.fiscal_yr_and_per_desc, lower(e.page_url);

-- Sheet 2, kolona C.1 # that opened TR or visited Challenges page this month
drop table if exists ra.tmp_open_tp_visit_challenges_month;
create temporary table ra.tmp_open_tp_visit_challenges_month as
select t.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as cnt_distinct_member
from ra.tmp_open_tp_visit_challenges t
where t.event_page_url in ('view','challenge')
group by t.fiscal_yr_and_per_desc;

-- Sheet 2, kolona D.1 # that downloaded resource or opened instruction card this month
drop table if exists ra.tmp_open_tp_download_open_ic_month;
create table ra.tmp_open_tp_download_open_ic_month
select t.fiscal_yr_and_per_desc,
	   count(distinct t.memberId) as cnt_distinct_member
from ra.tmp_open_tp_visit_challenges t
where t.event_page_url in ('download','instruction_card')
and exists (
    select 1 
    from ra.tmp_open_tp_visit_challenges b 
    where b.memberId = t.memberId
    and b.event_page_url in ('view','challenge')
    and b.fiscal_yr_and_per_desc = t.fiscal_yr_and_per_desc)
group by t.fiscal_yr_and_per_desc;


-- kolona E.1
-- Sheet 2, kolona E.1 # that login to Express in same month
create temporary table ra.tmp_log_in_to_express_month
select t.fiscal_yr_and_per_desc,
	   count(distinct t.memberId) as cnt_distinct_member
from ra.tmp_open_tp_visit_challenges t
where t.event_page_url in ('download','instruction_card')
and exists (
    select 1 
    from ra.tmp_open_tp_visit_challenges b 
    where b.memberId = t.memberId
    and b.event_page_url in ('view','challenge')
    and b.fiscal_yr_and_per_desc = t.fiscal_yr_and_per_desc)
and exists (
     select 1
     from hdp.spark_event_activity_b_logins e -- log in to express
     where 1=1
     and e.original_guid = t.userGuid COLLATE utf8mb4_general_ci
     and e.fiscal_yr_and_per = concat(left(t.fiscal_yr_and_per_desc,4),right(t.fiscal_yr_and_per_desc,2))
)
group by t.fiscal_yr_and_per_desc;



-- Sheet2, Kolona F.1 - visit express i acq content #% that acquired content AND logged into Express within same month
drop table if exists ra.tmp_acq_content_month_login_express;
create temporary table ra.tmp_acq_content_month_login_express
select t.fiscal_yr_and_per_desc,
	   count(distinct t.memberId) as cnt_distinct_member
from ra.tmp_open_tp_visit_challenges t
where t.event_page_url in ('download','instruction_card')
and exists (
    select 1 
    from ra.tmp_open_tp_visit_challenges b 
    where b.memberId = t.memberId
    and b.event_page_url in ('view','challenge')
    and b.fiscal_yr_and_per_desc = t.fiscal_yr_and_per_desc)
and exists (
     select 1
     from hdp.spark_event_activity_b_logins e -- log in to express
     where 1=1
     and e.original_guid = t.userGuid COLLATE utf8mb4_general_ci
     and e.fiscal_yr_and_per = concat(left(t.fiscal_yr_and_per_desc,4),right(t.fiscal_yr_and_per_desc,2))
)
and exists (
     select 1
     from ra.tmp_member_K12_acq_content e -- acq content (temporary smo je pravili)
     where 1=1
     and e.memberID = t.memberID
     and e.fiscal_yr_and_per_desc = t.fiscal_yr_and_per_desc
)
group by t.fiscal_yr_and_per_desc;


-- drugi deo
-- Sheet2, base tmp_open_course_details_page
drop table if exists ra.tmp_open_course_details_page;
create table ra.tmp_open_course_details_page as
select t.memberId,
       t.userGuid,
       b.fiscal_yr_and_per_desc,
       b.event as event_page_url
from ra.tmp_k12_members t
join ra.tmp_member_k12_loggedIn b on b.memberId =  t.memberId
where 1=1
and b.event = 'v1.course.fetched'
-- and b.event in ('v1.resource.fetched', 'resource.click.downloadToDevice')
group by t.memberId, t.userGuid, b.fiscal_yr_and_per_desc, b.eventLevel
union all
select e.memberId,
       t.userGuid as userGuid,
       b.fiscal_yr_and_per_desc,
       'enrollment' as event_page_url
from edx.Enrollment e
join ra.tmp_k12_members t on t.memberId = e.memberId
join ra.tmp_member_k12_loggedIn b on b.memberId =  t.memberId
join edx.hana_dim_date dd on dd.calendar_date = date(e.createdAt)
group by e.memberId, t.userGuid, b.fiscal_yr_and_per_desc;

-- Sheet2, kolona C.2 copy # that opened course details page this month
drop table if exists ra.tmp_open_course_detail_month;
create temporary table ra.tmp_open_course_detail_month as
select t.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as cnt_distinct_members
from ra.tmp_open_course_details_page t
where t.event_page_url = 'v1.course.fetched'
group by t.fiscal_yr_and_per_desc;

-- Sheet2, D.2 # that enrolled in a course this month
drop table if exists ra.tmp_enrolled_month;
create temporary table ra.tmp_enrolled_month
select t.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as cnt_distinct_members
from ra.tmp_open_course_details_page t
where t.event_page_url = 'enrollment'
and exists (
    select 1 
    from ra.tmp_open_course_details_page b 
    where b.memberId = t.memberId
    and b.event_page_url = 'v1.course.fetched'
    and b.fiscal_yr_and_per_desc = t.fiscal_yr_and_per_desc)
group by t.fiscal_yr_and_per_desc;

-- Sheet2, F.2 ## that login to Express in same month
drop table if exists ra.tmp_logIn_from_course_month;
create temporary table ra.tmp_logIn_from_course_month
select t.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as cnt_distinct_members
from ra.tmp_open_course_details_page t
where t.event_page_url = 'enrollment'
and exists (
    select 1 
    from ra.tmp_open_course_details_page b 
    where b.memberId = t.memberId
    and b.event_page_url = 'v1.course.fetched'
    and b.fiscal_yr_and_per_desc = t.fiscal_yr_and_per_desc)
and exists (
     select 1
     from hdp.spark_event_activity_b_logins e -- log in to express
     where 1=1
     and e.original_guid = t.userGuid COLLATE utf8mb4_general_ci
     and e.fiscal_yr_and_per = concat(left(t.fiscal_yr_and_per_desc,4),right(t.fiscal_yr_and_per_desc,2))
)
group by t.fiscal_yr_and_per_desc;

-- Sheet 2, G.2 # that acquired content AND logged into Express within same month
drop table if exists ra.tmp_logIn_acq_from_course_month;
create temporary table ra.tmp_logIn_acq_from_course_month
select t.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as cnt_distinct_members
from ra.tmp_open_course_details_page t
where t.event_page_url = 'enrollment'
and exists (
    select 1 
    from ra.tmp_open_course_details_page b 
    where b.memberId = t.memberId
    and b.event_page_url = 'v1.course.fetched'
    and b.fiscal_yr_and_per_desc = t.fiscal_yr_and_per_desc)
and exists (
     select 1
     from hdp.spark_event_activity_b_logins e -- log in to express
     where 1=1
     and e.original_guid = t.userGuid COLLATE utf8mb4_general_ci
     and  e.fiscal_yr_and_per = concat(left(t.fiscal_yr_and_per_desc,4),right(t.fiscal_yr_and_per_desc,2))
)
and exists (
     select 1
     from ra.tmp_member_K12_acq_content e -- acq content (temporary smo je pravili)
     where 1=1
     and e.memberID = t.memberID
     and e.fiscal_yr_and_per_desc = t.fiscal_yr_and_per_desc
)
group by t.fiscal_yr_and_per_desc;

-- final Sheet 2, Teaching resource, prvi deo
drop table if exists edx.rpt_conversion_benchmark_2;
create table edx.rpt_conversion_benchmark_2 as
select l.fiscal_yr_and_per_desc as "fiscal_month_year",
       l.cnt_distinct_members as "K12_edex_logged_members_total",
       cm.cnt_distinct_member as "open_TR_visit_challenge",
       ic.cnt_distinct_member as "download_tr_open_ic",
       lcm.cnt_distinct_member as "log_in_express",
       lac.cnt_distinct_member as "acquired_content_visited_express",
       lac.cnt_distinct_member / l.cnt_distinct_members  as "acquired_content_visited_express_perc"       
from ra.tmp_US_K12_LoggendIn l
join ra.tmp_open_tp_visit_challenges_month cm on cm.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
join ra.tmp_open_tp_download_open_ic_month ic on ic.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
-- join ra.tmp_log_in_to_express_month em on em.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
-- join ra.tmp_acq_content_month_login_express ac on ac.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
join ra.tmp_log_in_to_express_month lcm on lcm.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
join ra.tmp_acq_content_month_login_express lac on lac.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc;


-- final Sheet 2, Course, drugi deo
drop table if exists edx.rpt_conversion_benchmark_3;
create table edx.rpt_conversion_benchmark_3 as
select l.fiscal_yr_and_per_desc as "fiscal_month_year",
       l.cnt_distinct_members as "K12_edex_logged_members_total",
       c.cnt_distinct_members as "open_course_details",
       e.cnt_distinct_members as "enrolled_in_course",
       lcm.cnt_distinct_members as "log_in_express",
       lac.cnt_distinct_members as "acquired_content_visited_express",
       lac.cnt_distinct_members / l.cnt_distinct_members  as "acquired_content_visited_express_perc"       
from ra.tmp_US_K12_LoggendIn l
join ra.tmp_open_course_detail_month c on c.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
join ra.tmp_enrolled_month e on e.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
-- join ra.tmp_log_in_to_express_month em on em.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
-- join ra.tmp_acq_content_month_login_express ac on ac.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
--
join ra.tmp_logIn_from_course_month lcm on lcm.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc
join ra.tmp_logIn_acq_from_course_month lac on lac.fiscal_yr_and_per_desc = l.fiscal_yr_and_per_desc;


-- select *
-- from edx.rpt_conversion_benchmark_3


-- =============================== potencijalno za izmenu
-- kolona E.2
-- kolona E.2 (od totala koliko visit Express)
drop table if exists ra.tmp_log_in_to_express_month_e2;
create temporary table ra.tmp_log_in_to_express_month_e2
select b.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as count_
from ra.tmp_k12_members t
join ra.tmp_member_k12_loggedIn b on b.memberId =  t.memberId
where exists (
select 1
from hdp.spark_event_activity_b_logins e -- log in to express
where 1=1
and e.original_guid = t.userGuid COLLATE utf8mb4_general_ci
and  concat(left(e.fiscal_yr_and_per, 4),'-',right(e.fiscal_yr_and_per, 2)) = b.fiscal_yr_and_per_desc
-- and  e.fiscal_yr_and_per  = b.fiscal_yr_and_per ispraviti, promeniti i popraviti
)
group by b.fiscal_yr_and_per_desc;

-- Kolona F.2 - visit express i acq content
drop table if exists ra.tmp_acq_content_month_login_express_f2;
create temporary table ra.tmp_acq_content_month_login_express_f2
select b.fiscal_yr_and_per_desc,
       count(distinct t.memberId) as count_
from ra.tmp_k12_members t
join ra.tmp_member_k12_loggedIn b on b.memberId =  t.memberId
where exists (
select 1
from hdp.spark_event_activity_b_logins e -- log in to express
where 1=1
and e.original_guid = t.userGuid COLLATE utf8mb4_general_ci
and  concat(left(e.fiscal_yr_and_per, 4),'-',right(e.fiscal_yr_and_per, 2)) = b.fiscal_yr_and_per_desc
-- and  e.fiscal_yr_and_per  = b.fiscal_yr_and_per ispraviti, promeniti i popraviti
)
and exists (
select 1
from ra.tmp_member_K12_acq_content e -- acq content (temporary smo je pravili)
where 1=1
and e.memberID = t.memberID
and  e.fiscal_yr_and_per_desc = b.fiscal_yr_and_per_desc
-- and  e.fiscal_yr_and_per  = b.fiscal_yr_and_per ispraviti, promeniti i popraviti
)
group by b.fiscal_yr_and_per_desc;
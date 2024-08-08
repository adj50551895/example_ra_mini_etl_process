-- May contain fiscal months gap (@current_fiscal_yr_and_per)
-- If the date of the last start of the report and the current date of the start of the report are in a different fiscal month, it is necessary to start the report twice - for the last two fiscal months.

-- set @date_from = '2021-12-03';
select date(param_value)
into @date_from
from edx.rpt_params
where report_name = 'rpt_content_detail' 
and param_name = 'course_rpt_start_date';

set @date_from = '2020-11-27';


-- base member table
drop table if exists ra.tmp_content_detail_course_base_members;
create table ra.tmp_content_detail_course_base_members as
select m.id as memberId,
       case when d.district_id is null then 0
            else d.district_id
       end as district_id,
       case when d.school_district_name is null then 'No District Information'
            else d.school_district_name
       end as school_district_name,
       d.domain,
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
       case when d.vsky = '0' then 0
            when d.vsky is null then 0
            else d.vsky
       end as vsky,
       case when d.high_rise = '0' then 0
            when d.high_rise is null then 0
            else d.high_rise
       end as high_rise,
       trim(mp.userGuid) as userGuid,
       m.createdAt as memberCreatedAt,
       case when m.countryCode = 'US' then m.countryCode
            when l.countryCode = 'US' then l.countryCode
       end as countryCode
from edx.Member m
join hdp.memberguidmap mp on mp.memberId = m.id
left join edx.district_domains d on d.domain COLLATE utf8mb4_general_ci = lower(SUBSTRING_INDEX(m.email, "@", -1))
left join edx.ud_lookup l on l.memberId = m.id;
-- where 1=1
-- and exists (select 1 from edx.MemberSegmentation s where s.memberId = m.id and s.class = 'K12');

delete from ra.tmp_content_detail_course_base_members where countryCode is null;

-- reduced events table
drop table if exists ra.tmp_content_detail_events;
create table ra.tmp_content_detail_events as
select e.event_date, e.id, e.entityID, e.event, e.event_timestamp, e.entityType, e.eventLevel, e.memberId, e.sessionId
from els.events e
where e.entityType = 'course'
and e.eventLevel in ('engage','view')
and e.event_date > @date_from;

-- reduced events table
insert into ra.tmp_content_detail_events
select e.event_date, e.id, e.entityID, e.event, e.event_timestamp, e.entityType, e.eventLevel, e.memberId, e.sessionId
from els.events e
where e.event in ('course.click.otherLink.inline', 'v1.resource.fetched')
and e.event_date > @date_from
and not exists (select 1 from ra.tmp_content_detail_events e1 where e1.id = e.id);


-- 1
drop table if exists ra.tmp_content_detail_courses_views;
create table ra.tmp_content_detail_courses_views as
select dd.fiscal_yr_and_per,
       dom.district_id,
       dom.school_district_name,
       e.entityID as courseID,
       dom.sky_scraper,
       dom.top_500,
       dom.top_200,
       dom.top_100,
       dom.pod,
       dom.vsky,
       dom.high_rise,
       sum(case when e.eventLevel = 'view' then 1 end) as total_views,
       COALESCE(sum(case when e.eventLevel = 'view' and e.memberID is null then 1 end), 0) as guest_views,
       -- #Percent of Views made by Guests
       sum(case when e.eventLevel = 'view' and e.memberID is not null then 1 end) as member_views,
       count(distinct case when e.eventLevel = 'view' then concat(ifnull(e.memberID,e.sessionID),e.event_date) end) as unique_daily_views,
       count(distinct case when e.eventLevel = 'view' then ifnull(e.memberID,e.sessionID) end) as unique_views,
       count(distinct case when e.eventLevel = 'download' then e.memberID end) as unique_downloads
       -- #Rate of Completion by those that Started
from ra.tmp_content_detail_events e
join ra.tmp_content_detail_course_base_members dom on dom.memberId = e.memberId
join edx.hana_dim_date dd on dd.calendar_date = e.event_date
where e.entityType = 'course'
and e.eventLevel in ('engage','view')
and e.entityID is not null
group by dd.fiscal_yr_and_per,
         dom.district_id,
         dom.school_district_name,
         e.entityID,
         dom.sky_scraper,
         dom.top_500,
         dom.top_200,
         dom.top_100,
         dom.pod,
         dom.vsky,
         dom.high_rise;


-- 2
drop table if exists ra.tmp_content_detail_courses_enrollments;
create table ra.tmp_content_detail_courses_enrollments as
select dd.fiscal_yr_and_per,
       dom.district_id,
       dom.school_district_name,
       dom.sky_scraper,
       dom.top_500,
       dom.top_200,
       dom.top_100,
       dom.pod,
       dom.vsky,
       dom.high_rise,
       e.courseId,
       count(distinct e.memberId) as enrolled_quantity, -- "Enrolled Quantity",
       count(distinct case when e.status = 'enrolled' then e.memberId end) as course_enrolled, -- Course Enrolled
       count(distinct case when e.status = 'started' then e.memberId end) as course_started, -- Course Starts
       count(distinct case when e.status = 'passed' then e.memberId end) as course_passed, -- Completed Quantity, Graduation??
       count(distinct case when (e.status = 'started' or e.status = 'passed') then e.memberId end) as course_started_passed,
       count(distinct case when (e.status != 'started' and e.status != 'passed' and e.status != 'enrolled') then e.memberId end) as course_removed_review_incomlete
from edx.Enrollment e
join ra.tmp_content_detail_course_base_members dom on dom.memberId = e.memberId
join edx.Course c on c.id = e.courseId
join edx.hana_dim_date dd on dd.calendar_date = date(e.createdAt)
where 1=1
and date(e.createdAt) > @date_from
group by dd.fiscal_yr_and_per,
         dom.district_id,
         dom.school_district_name,
         dom.sky_scraper,
         dom.top_500,
         dom.top_200,
         dom.top_100,
         dom.pod,
         dom.vsky,
         dom.high_rise,
         e.courseId;


-- 3 Link to Express and CC use from courses (content acquisition & MAU) 
-- Link to Express
drop table if exists ra.tmp_content_detail_courses_link_to_express;
create table ra.tmp_content_detail_courses_link_to_express as
select distinct e.memberId,
                dom.district_id,
                dom.school_district_name,
                dom.sky_scraper,
                dom.top_500,
                dom.top_200,
                dom.top_100,
                dom.pod,
                dom.vsky,
                dom.high_rise,
                c.id as courseId,
                t.max_event_date as express_login,
                date(e.createdAt) as enrollemnt_started,
                t.fiscal_yr_and_per as fiscal_yr_and_per_hana_t,
                dd.fiscal_yr_and_per as fiscal_yr_and_per_hana_dd
from ra.tmp_content_detail_course_base_members dom
join hdp.spark_event_activity_b_logins t on t.original_guid COLLATE utf8mb4_general_ci = dom.userGuid
join edx.Enrollment e on e.memberId = dom.memberId
join edx.hana_dim_date dd on dd.calendar_date =date(e.createdAt)
join edx.Course c on c.id = e.courseId
where 1=1
and t.max_event_date >= date(e.createdAt)
-- and t.fiscal_yr_and_per = dd.fiscal_yr_and_per
and date(e.createdAt) > @date_from;

-- Link to CC
-- 1
drop table if exists ra.tmp_content_detail_courses_link_to_cc_dom;
create table ra.tmp_content_detail_courses_link_to_cc_dom as
select distinct dom.memberId,
                dom.district_id,
                dom.school_district_name,
                dom.sky_scraper,
                dom.top_500,
                dom.top_200,
                dom.top_100,
                dom.pod,
                dom.vsky,
                dom.high_rise,
                t.max_activity_date,
                t.fiscal_yr_and_per
from ra.tmp_content_detail_course_base_members dom
join hdp.ccmusg_fact_user_activity_cc_dc t on t.user_guid COLLATE utf8mb4_general_ci = dom.userGuid
where 1 = 1
and t.product_category = 'CC';

-- 2
drop table if exists ra.tmp_content_detail_courses_link_to_cc;
create table ra.tmp_content_detail_courses_link_to_cc as
select distinct e.memberId,
                dom.district_id,
                dom.school_district_name,
                dom.sky_scraper,
                dom.top_500,
                dom.top_200,
                dom.top_100,
                dom.pod,
                dom.vsky,
                dom.high_rise,
                c.id as courseId,
                dom.max_activity_date as cc_login,
                date(e.createdAt) as enrollemnt_started,
                dom.fiscal_yr_and_per as fiscal_yr_and_per_hana_t,
                dd.fiscal_yr_and_per as fiscal_yr_and_per_hana_dd
from ra.tmp_content_detail_courses_link_to_cc_dom dom
join edx.Enrollment e on e.memberId = dom.memberId
join edx.hana_dim_date dd on dd.calendar_date = date(e.createdAt)
join edx.Course c on c.id = e.courseId
where 1=1
and dom.max_activity_date >= date(e.createdAt)
-- and t.fiscal_yr_and_per = dd.fiscal_yr_and_per
and date(e.createdAt) > @date_from;


drop table if exists ra.tmp_content_detail_courses_link_to_express_cc;
create table ra.tmp_content_detail_courses_link_to_express_cc as
select t.fiscal_yr_and_per,
       t.district_id,
       t.school_district_name,
       t.sky_scraper,
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.vsky,
       t.high_rise,
       t.courseId,
       count(distinct t.memberId) as distinct_member_cnt
from (
select ex.memberId,
       ex.fiscal_yr_and_per_hana_dd as fiscal_yr_and_per,
       ex.district_id,
       ex.school_district_name,
       ex.sky_scraper,
       ex.top_500,
       ex.top_200,
       ex.top_100,
       ex.pod,
       ex.vsky,
       ex.high_rise,
       ex.courseId
from ra.tmp_content_detail_courses_link_to_express ex
where ex.fiscal_yr_and_per_hana_t = ex.fiscal_yr_and_per_hana_dd
union all
select cc.memberId,
       cc.fiscal_yr_and_per_hana_dd as fiscal_yr_and_per,
       cc.district_id,
       cc.school_district_name,
       cc.sky_scraper,
       cc.top_500,
       cc.top_200,
       cc.top_100,
       cc.pod,
       cc.vsky,
       cc.high_rise,
       cc.courseId
from ra.tmp_content_detail_courses_link_to_cc cc
where cc.fiscal_yr_and_per_hana_t = cc.fiscal_yr_and_per_hana_dd) t
group by t.fiscal_yr_and_per,
         t.sky_scraper,
         t.top_500,
         t.top_200,
         t.top_100,
         t.pod,
         t.vsky,
         t.high_rise,
         t.courseId;
-- // 3 Link to Express and CC use from courses (content acquisition & MAU) 


-- 4 Clicks to templates from courses
drop table if exists ra.tmp_content_detail_courses_click_on_templates;
create table ra.tmp_content_detail_courses_click_on_templates as
select dd.fiscal_yr_and_per,
       dom.district_id,
       dom.school_district_name,
       dom.sky_scraper,
       dom.top_500,
       dom.top_200,
       dom.top_100,
       dom.pod,
       dom.vsky,
       dom.high_rise,
       e.courseId,
       count(distinct e.memberId)  as distinct_member_cnt       
from els.events e
join ra.tmp_content_detail_course_base_members dom on dom.memberId = e.memberId
join edx.hana_dim_date dd on dd.calendar_date = date(e.event_date)
where e.event in ('course.click.ccxTemplateLink', 'course.click.ccxTemplateLink.inline')
and e.event_date > @date_from
group by dd.fiscal_yr_and_per,
         dom.district_id,
         dom.school_district_name,
         dom.sky_scraper,
         dom.top_500,
         dom.top_200,
         dom.top_100,
         dom.pod,
         dom.vsky,
         dom.high_rise,
         e.courseId;
-- // 4 Clicks to templates from courses


-- 5 Clicks to teaching resources from courses
drop table if exists ra.tmp_content_detail_resources_from_courses_events;
create table ra.tmp_content_detail_resources_from_courses_events as
select e1.memberId,
       e1.event_date as event_date_1,
       e2.event_date as event_date_2,
       e1.event as event_1,
       e2.event as event_2,
       e1.entityID as entityID_1, -- courseId
       e2.entityID as entityID_2,
       e1.id as id_1,
       e2.id as id_2,
       timediff(e2.event_timestamp, e1.event_timestamp) as event_timestamp_diff,
       ROW_NUMBER() OVER (PARTITION BY e1.memberId, e2.id ORDER BY timediff(e2.event_timestamp, e1.event_timestamp)) row_num -- e2.id umseto e2.entity_id
from ra.tmp_content_detail_events e1
join ra.tmp_content_detail_events e2 on e1.memberId = e2.memberId and e1.sessionId = e2.sessionId
-- join ra.tmp_content_detail_course_base_members dom on dom.memberId = e1.memberId
-- join edx.hana_dim_date dd on dd.calendar_date = date(e1.event_date)
where e1.event = 'course.click.otherLink.inline'
and e2.event =  'v1.resource.fetched'
and e1.event_timestamp < e2.event_timestamp
and e1.event_date > @date_from
and e2.event_date > @date_from;

drop table if exists ra.tmp_content_detail_resources_from_courses;
create table ra.tmp_content_detail_resources_from_courses as
select dd.fiscal_yr_and_per,
       dom.district_id,
       dom.school_district_name,
       dom.sky_scraper,
       dom.top_500,
       dom.top_200,
       dom.top_100,
       dom.pod,
       dom.vsky,
       dom.high_rise,
       t.entityId_1 as courseId,
       count(distinct t.memberId) as distinct_members_cnt
from ra.tmp_content_detail_resources_from_courses_events t
join ra.tmp_content_detail_course_base_members dom on dom.memberId = t.memberId
join edx.hana_dim_date dd on dd.calendar_date = date(t.event_date_1)
group by dd.fiscal_yr_and_per,
         dom.district_id,
         dom.school_district_name,
         dom.sky_scraper,
         dom.top_500,
         dom.top_200,
         dom.top_100,
         dom.pod,
         dom.vsky,
         dom.high_rise,
         t.entityId_1;
-- // 5 Clicks to teaching resources from courses


-- 6 Member sign up to course enrollment relationship at the same date
drop table if exists ra.tmp_content_detail_signup_to_course_enrll_relation;
create table ra.tmp_content_detail_signup_to_course_enrll_relation as
select dd.fiscal_yr_and_per,
       dom.district_id,
       dom.school_district_name,
       dom.sky_scraper,
       dom.top_500,
       dom.top_200,
       dom.top_100,
       dom.pod,
       dom.vsky,
       dom.high_rise,
       e.courseId,
       count(distinct e.memberId) as distinct_members_cnt -- , e.createdAt, dom.memberCreatedAt, timediff(e.createdAt, dom.memberCreatedAt), datediff(e.createdAt, dom.memberCreatedAt)
from edx.Enrollment e
join ra.tmp_content_detail_course_base_members dom on dom.memberId = e.memberId
join edx.hana_dim_date dd on dd.calendar_date = date(dom.memberCreatedAt)
where 1=1
and datediff(e.createdAt, dom.memberCreatedAt) = 0
and e.createdAt > @date_from
group by dd.fiscal_yr_and_per,
         dom.district_id,
         dom.school_district_name,
         dom.sky_scraper,
         dom.top_500,
         dom.top_200,
         dom.top_100,
         dom.pod,
         dom.vsky,
         dom.high_rise,
         e.courseId;
-- // 6 Member sign up to course enrollment relationship

-- 7 Lighting Learning Courses
-- ACE and Lightning Learning Courses
-- ACE L1 is any course with the title "Creativity for all" and course ID : b8c27c36-3281-4e9e-af93-466107644d83 or 5d87b072-37f0-4138-8133-ae8e9d4870a9
-- These are Lightning Learning courses on Adobe’s webpage: https://git.corp.adobe.com/education-data-science/New_Express
drop temporary table if exists ra.tmp_member_content_detail_courses_ace_llc;
create temporary table ra.tmp_member_content_detail_courses_ace_llc as
select c.id as courseId, 'ACE' as course_label
from edx.Course c
where 1=1
and (c.title like '%Creativity%for%all%' or c.id in ('b8c27c36-3281-4e9e-af93-466107644d83', '5d87b072-37f0-4138-8133-ae8e9d4870a9'))
union all
select c.id as courseId, 'LLC' as course_label
from edx.Course c
where 1=1
and c.id in (
'082cb385-5453-40cf-be82-b25b72d0c1e2',
'51ef2930-bc92-420a-bc2e-d2849775b42f',
'36610987-6647-428a-8d6b-5788061a9357',
'733ec6b7-50d5-4c64-8f0b-5491f7906bfd',
'8f62820a-49f2-434b-acf9-d477c4d67701',
'e1a50170-37be-45be-83d2-01361ede97ac',
'eaccf4a0-9493-4aec-9c91-c363af8a0e4a',
'84817be1-9cc0-46cb-9bd5-7b8eb42129a0',
'a544409a-18ab-4905-bc30-75ff6ca695ba',
'd7e95027-4f30-440d-863c-338670de37b4',
'e2c34f43-ad16-4698-8476-e39e26ec3472',
'b9e3acb3-eae2-4034-ae9b-7a0bbaf208c3',
'46bbab85-7e9a-4d4f-a73c-6d0827cdff9f',
'ed21c307-67ce-472a-8484-590e6a84c372');
-- //7 ACE and Lighting Learning Courses

-- final table preparation
drop table if exists ra.tmp_content_detail_final_base;
create table ra.tmp_content_detail_final_base as
select fiscal_yr_and_per, sky_scraper, top_500, top_200, top_100, pod, vsky, high_rise, courseId, district_id, school_district_name
from ra.tmp_content_detail_courses_views
union 
select fiscal_yr_and_per, sky_scraper, top_500, top_200, top_100, pod, vsky, high_rise, courseId, district_id, school_district_name
from ra.tmp_content_detail_courses_enrollments
union
select fiscal_yr_and_per, sky_scraper, top_500, top_200, top_100, pod, vsky, high_rise, courseId, district_id, school_district_name
from ra.tmp_content_detail_courses_link_to_express_cc
union
select fiscal_yr_and_per, sky_scraper, top_500, top_200, top_100, pod, vsky, high_rise, courseId, district_id, school_district_name
from ra.tmp_content_detail_courses_click_on_templates
union
select fiscal_yr_and_per, sky_scraper, top_500, top_200, top_100, pod, vsky, high_rise, courseId, district_id, school_district_name
from ra.tmp_content_detail_resources_from_courses
union
select fiscal_yr_and_per, sky_scraper, top_500, top_200, top_100, pod, vsky, high_rise, courseId, district_id, school_district_name
from ra.tmp_content_detail_signup_to_course_enrll_relation
order by 1, 5;


-- final table
update edx.rpt_content_detail_course set isCurrent = '0' where isCurrent = '1';

-- drop table if exists edx.rpt_content_detail_course;
-- create table edx.rpt_content_detail_course_28022023_2 as
insert into edx.rpt_content_detail_course
select base.fiscal_yr_and_per,
       concat(left(base.fiscal_yr_and_per,4),'-',right(base.fiscal_yr_and_per,2)) as fiscal_yr_and_per_desc,
       base.district_id,
       base.school_district_name,
       base.courseId, -- "Course Id"
       case when base.sky_scraper = 1 then 'Sky Scraper'
            else 'Non-Sky Scraper'
       end as "sky_scraper",
       case when base.top_500 = 1 then 'Top 500'
            else 'Non-Top 500'
       end as "top_500",
       case when base.top_200 = 1 then 'Top 200'
            else 'Non-Top 200'
       end as "top_200",
       case when base.top_100 = 1 then 'Top 100'
            else 'Non-Top 100'
       end as "top_100",
       trim(base.pod) as "pod",
       case when base.vsky = 0 then 'Non-V Sky'
            else 'V Sky'
       end as "V Sky",
       case when base.high_rise = 0 then 'Non-High Rise'
            else 'High Rise'
       end as "High Rise",
       ifnull(llc.course_label, 'Non-ACE LLC') as ace_llc,
       c.title,
       c.type as course_type, -- "Course Type"
       date(c.publishAt) as publish_date, -- "Publish Date",
       c.difficulty, -- "Difficulty"
       c.vanityURL,
       c.status,
       coalesce(vw.total_views, 0) as total_views, -- "Total Views"
       coalesce(vw.guest_views, 0) as guest_views, -- "Guest Views"
       coalesce(vw.guest_views / vw.total_views, 0) as percent_guest_views, -- "Percent of Views made by Guests"
       coalesce(vw.member_views, 0) as member_views, -- "Member Views"
       coalesce(vw.member_views / vw.total_views, 0) as percent_member_views, -- "Percent of Views made by Members"
       coalesce(vw.unique_daily_views, 0) as unique_daily_views, -- "Unique Daily Views"
       coalesce(vw.unique_views, 0) as unique_member_views, -- "Unique Member Views"
       coalesce(vw.unique_downloads, 0) as unique_downloads, -- "Unique Downloads"
       coalesce(enr.enrolled_quantity, 0) as enrolled_quantity, -- "Enrolled Quantity"
       coalesce(enr.course_enrolled, 0) as course_enrolled, -- "Course Enrolled"
       coalesce(enr.course_started, 0) as course_stars, -- "Course Starts"
       coalesce(enr.course_passed, 0) as graduation, -- "Graduation"
       coalesce(enr.course_removed_review_incomlete, 0) as course_removed_review_incomlete,
       -- coalesce(enr.course_started_passed, 0) as "course_started_passed"
       coalesce(enr.course_passed / enr.course_started_passed, 0) as rate_completion_started, -- "Rate of Completion by those that Started"
       coalesce(exp_cc.distinct_member_cnt, 0) as link_to_express_and_cc, -- "Link to Express and CC use from courses"
       coalesce(tmpl.distinct_member_cnt, 0) as  clicks_to_templates, -- "Clicks to templates from courses" -- Inform Sarah
       coalesce(tr.distinct_members_cnt, 0) as clicks_to_teaching_resources, -- "Clicks to teaching resources from courses"
       coalesce(er.distinct_members_cnt, 0) as member_sign_up_course_enrollment, -- "Member sign up to course enrollment relationship"
       -- #Rate of Completion by those that Started
       current_timestamp() as createdAt,
       1 as isCurrent
from ra.tmp_content_detail_final_base base
join edx.Course c on c.id = base.courseId
left join ra.tmp_member_content_detail_courses_ace_llc llc on llc.courseId = base.courseId
left join ra.tmp_content_detail_courses_views vw on vw.fiscal_yr_and_per = base.fiscal_yr_and_per
                                                and vw.sky_scraper = base.sky_scraper
                                                and vw.top_500 = base.top_500
                                                and vw.top_200 = base.top_200
                                                and vw.top_100 = base.top_100
                                                and vw.pod = base.pod
                                                and vw.vsky = base.vsky
                                                and vw.high_rise = base.high_rise
                                                and vw.courseId = base.courseId
                                                and vw.district_id = base.district_id
left join ra.tmp_content_detail_courses_enrollments enr on enr.fiscal_yr_and_per = base.fiscal_yr_and_per
                                                       and enr.sky_scraper = base.sky_scraper
                                                       and enr.top_500 = base.top_500
                                                       and enr.top_200 = base.top_200
                                                       and enr.top_100 = base.top_100
                                                       and enr.pod = base.pod
                                                       and enr.vsky = base.vsky
                                                       and enr.high_rise = base.high_rise
                                                       and enr.courseId = base.courseId
                                                       and enr.district_id = base.district_id
left join ra.tmp_content_detail_courses_link_to_express_cc exp_cc on exp_cc.fiscal_yr_and_per = base.fiscal_yr_and_per
                                                                 and exp_cc.sky_scraper = base.sky_scraper
                                                                 and exp_cc.top_500 = base.top_500
                                                                 and exp_cc.top_200 = base.top_200
                                                                 and exp_cc.top_100 = base.top_100
                                                                 and exp_cc.pod = base.pod
                                                                 and exp_cc.vsky = base.vsky
                                                                 and exp_cc.high_rise = base.high_rise
                                                                 and exp_cc.courseId = base.courseId
                                                                 and exp_cc.district_id = base.district_id
left join ra.tmp_content_detail_courses_click_on_templates tmpl on tmpl.fiscal_yr_and_per = base.fiscal_yr_and_per
                                                               and tmpl.sky_scraper = base.sky_scraper
                                                               and tmpl.top_500 = base.top_500
                                                               and tmpl.top_200 = base.top_200
                                                               and tmpl.top_100 = base.top_100
                                                               and tmpl.pod = base.pod
                                                               and tmpl.vsky = base.vsky
                                                               and tmpl.high_rise = base.high_rise
                                                               and tmpl.courseId = base.courseId
                                                               and tmpl.district_id = base.district_id
left join ra.tmp_content_detail_resources_from_courses tr on tr.fiscal_yr_and_per = base.fiscal_yr_and_per
                                                         and tr.sky_scraper = base.sky_scraper
                                                         and tr.top_500 = base.top_500
                                                         and tr.top_200 = base.top_200
                                                         and tr.top_100 = base.top_100
                                                         and tr.pod = base.pod
                                                         and tr.vsky = base.vsky
                                                         and tr.high_rise = base.high_rise
                                                         and tr.courseId = base.courseId
                                                         and tr.district_id = base.district_id
left join ra.tmp_content_detail_signup_to_course_enrll_relation er on er.fiscal_yr_and_per = base.fiscal_yr_and_per
                                                                  and er.sky_scraper = base.sky_scraper
                                                                  and er.top_500 = base.top_500
                                                                  and er.top_200 = base.top_200
                                                                  and er.top_100 = base.top_100
                                                                  and er.pod = base.pod
                                                                  and er.vsky = base.vsky
                                                                  and er.high_rise = base.high_rise
                                                                  and er.courseId = base.courseId
                                                                  and er.district_id = base.district_id
order by 1, 2, 3, 4;


-- 28.04.2023 because of the history data,fiscal month gap
-- current month
select dd.fiscal_yr_and_per
into @current_fiscal_yr_and_per
from edx.hana_dim_date dd 
where dd.calendar_date = current_date();

-- prev month
select max(fiscal_yr_and_per)
into @report_fiscal_yr_and_per
from edx.rpt_content_detail_course_fin;

select if(@current_fiscal_yr_and_per != @report_fiscal_yr_and_per, 1, 0)
into @prev_month;

-- set @current_fiscal_yr_and_per = 202305;
-- set @prev_month = 0;

-- prev month
delete from edx.rpt_content_detail_course_fin where fiscal_yr_and_per = @report_fiscal_yr_and_per and 1=@prev_month;

insert into edx.rpt_content_detail_course_fin
select *
from edx.rpt_content_detail_course t
where isCurrent = 1
and fiscal_yr_and_per = @report_fiscal_yr_and_per
and 1=@prev_month;

-- current month
delete from edx.rpt_content_detail_course_fin where fiscal_yr_and_per = @current_fiscal_yr_and_per;

insert into edx.rpt_content_detail_course_fin
select *
from edx.rpt_content_detail_course t
where isCurrent = 1
and fiscal_yr_and_per = @current_fiscal_yr_and_per;
-- //28.04.2023 because of the history data

-- drop tmp tables
-- drop table if exists ra.tmp_content_detail_course_base_members;
drop table if exists ra.tmp_content_detail_events;
drop table if exists ra.tmp_content_detail_courses_views;
drop table if exists ra.tmp_content_detail_courses_enrollments;
drop table if exists ra.tmp_content_detail_courses_link_to_express;
drop table if exists ra.tmp_content_detail_courses_link_to_cc;
drop table if exists ra.tmp_content_detail_courses_link_to_express_cc;
drop table if exists ra.tmp_content_detail_courses_click_on_templates;
drop table if exists ra.tmp_content_detail_resources_from_courses_events;
drop table if exists ra.tmp_content_detail_resources_from_courses;
drop table if exists ra.tmp_content_detail_signup_to_course_enrll_relation;
drop table if exists ra.tmp_content_detail_final_base;
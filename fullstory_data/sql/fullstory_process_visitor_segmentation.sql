-- 1. check parameter @fiscal_month in edx.rpt_params table
-- 2. run fullstory_load_source_data_page_details.py
-- 3. run fullstory_process1.py
-- 4. run fullstory_process_visitor_segmentation.sql

select param_value
into @fiscal_month
from edx.rpt_params
where report_name = 'rpt_fullstory_visitors' 
and param_name = 'rpt_fiscal_month';

-- drop table if exists ra.fullstory_visitor_segmenatation_temp;
delete from ra.fullstory_visitor_segmenatation_temp;

-- 1 Member discussion comments
insert into ra.fullstory_visitor_segmenatation_temp 
select distinct t.IndvId,
       'nullnullnull' as eduLevel,
       SUBSTRING(t.pageUrl, LOCATE('discussion/', t.pageUrl) + LENGTH('discussion/'), LENGTH(t.pageUrl)) as note,
       'member_edex_adobe_com_discussion' as rule_name,
       t.fiscal_yr_and_per,
       1 as count,
       1 as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageURL like '%edex.adobe.com/community/discussion/%';

-- 2 Member comments
insert into ra.fullstory_visitor_segmenatation_temp
select distinct t.IndvId,
       'nullnullnull' as eduLevel,
       SUBSTRING(t.pageUrl, LOCATE('edex.adobe.com/comment/', t.pageUrl) + LENGTH('edex.adobe.com/comment/'), 36) as note,
       'member_edex_adobe_com_comment' as rule_name,
       t.fiscal_yr_and_per,
       1 as count,
       1 as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.PageUrl like '%edex.adobe.com/comment/%';

-- 3 Teaching resources
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       t.layer as note,
       'teaching_resources' as rule_name,
       t.fiscal_yr_and_per,
       count(*) as count,
       count(*) OVER (PARTITION BY IndvId) as count_partition
from ra.fullstory_tmp_visitor_resources t
where t.eduLevel is not null
group by t.IndvId,
         t.eduLevel,
         t.layer;

-- 4 Courses
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       'primary' as note,
       'courses' as rule_name,
       t.fiscal_yr_and_per,
       count(*) as count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitor_courses t
where t.eduLevel is not null
group by t.IndvId,
         t.eduLevel;

-- 5 Member home pages (community)
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       'nullnullnull' as eduLevel,
       SUBSTRING(t.pageUrl, LOCATE('community/member/', t.pageUrl) + LENGTH('community/member/'), LENGTH(t.pageUrl)) as note,
       'member_edex_adobe_com_member' as rule_name,
       t.fiscal_yr_and_per,
       1 as count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageURL like  '%community/member/%';

-- 6 Search K12
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       'K12' as eduLevel,
       'seach_k12' as note,
       'search'as rule_name,
       t.fiscal_yr_and_per,
       1 count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageUrl like '%search?%age_level%'
and t.pageUrl not like '%Higher_Education%'
and t.pageUrl not like '%All_Ages%';

-- 7 Search HED
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       'HED' as eduLevel,
       'seach_hed' as note,
       'search'as rule_name,
       t.fiscal_yr_and_per,
       1 count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageUrl like '%search?%age_level%'
and t.pageUrl like '%Higher_Education%'
and t.pageUrl not like '%All_Ages%';

-- 8 Teaching resources all
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       'nullnullnull' as eduLevel,
       SUBSTRING(t.pageUrl, LOCATE('edex.adobe.com/teaching-resources/', t.pageUrl) + LENGTH('edex.adobe.com/teaching-resources/'), LENGTH(t.pageUrl)) as note,
       'teaching_resources_all' as rule_name,
       t.fiscal_yr_and_per,
       1 as count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageUrl like '%edex.adobe.com/teaching-resources/%';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '/', 1)
where t.rule_name = 'teaching_resources_all';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '?', 1)
where t.rule_name = 'teaching_resources_all';

-- 9 Course self-paced-course
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       'nullnullnull' as eduLevel,
       SUBSTRING(t.pageUrl, LOCATE('edex.adobe.com/professional-learning/self-paced-course/', t.pageUrl) + LENGTH('edex.adobe.com/professional-learning/self-paced-course/'), LENGTH(t.pageUrl)) as note,
       'courses_all_self_paced_course' as rule_name,
       t.fiscal_yr_and_per,
       1 as count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageUrl like '%edex.adobe.com/professional-learning/self-paced-course/%';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '/', 1)
where t.rule_name = 'courses_all_self_paced_course';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '?', 1)
where t.rule_name = 'courses_all_self_paced_course';

-- 10 Course live-instructor-led-course
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       'nullnullnull' as eduLevel,
       SUBSTRING(t.pageUrl, LOCATE('edex.adobe.com/professional-learning/live-instructor-led-course/', t.pageUrl) + LENGTH('edex.adobe.com/professional-learning/live-instructor-led-course/'), LENGTH(t.pageUrl)) as note,
       'courses_all_live_instructor_led_course' as rule_name,
       t.fiscal_yr_and_per,
       1 as count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageUrl like '%edex.adobe.com/professional-learning/live-instructor-led-course/%';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '/', 1)
where t.rule_name = 'courses_all_live_instructor_led_course';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '?', 1)
where t.rule_name = 'courses_all_live_instructor_led_course';

-- 11 Course instructor-led-course
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       'nullnullnull' as eduLevel,
       SUBSTRING(t.pageUrl, LOCATE('edex.adobe.com/professional-learning/instructor-led-course/', t.pageUrl) + LENGTH('edex.adobe.com/professional-learning/instructor-led-course/'), LENGTH(t.pageUrl)) as note,
       'courses_all_instructor_led_course' as rule_name,
       t.fiscal_yr_and_per,
       1 as count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageUrl like '%edex.adobe.com/professional-learning/instructor-led-course/%';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '/', 1)
where t.rule_name = 'courses_all_instructor_led_course';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '?', 1)
where t.rule_name = 'courses_all_instructor_led_course';

-- 12 Course toolkit
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       'nullnullnull' as eduLevel,
       SUBSTRING(t.pageUrl, LOCATE('edex.adobe.com/professional-learning/toolkit/', t.pageUrl) + LENGTH('edex.adobe.com/professional-learning/toolkit/'), LENGTH(t.pageUrl)) as note,
       'courses_all_toolkit' as rule_name,
       t.fiscal_yr_and_per,
       1 as count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageUrl like '%edex.adobe.com/professional-learning/toolkit/%';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '/', 1)
where t.rule_name = 'courses_all_toolkit';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '?', 1)
where t.rule_name = 'courses_all_toolkit';

-- 13 Course toolkit
insert into ra.fullstory_visitor_segmenatation_temp
select distinct 
       t.IndvId,
       'nullnullnull' as eduLevel,
       SUBSTRING(t.pageUrl, LOCATE('edex.adobe.com/professional-learning/undefined/', t.pageUrl) + LENGTH('edex.adobe.com/professional-learning/undefined/'), LENGTH(t.pageUrl)) as note,
       'courses_all_undefined' as rule_name,
       t.fiscal_yr_and_per,
       1 as count,
       count(*) OVER (PARTITION BY t.IndvId) as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.pageUrl like '%edex.adobe.com/professional-learning/undefined/%';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '/', 1)
where t.rule_name = 'courses_all_undefined';

update ra.fullstory_visitor_segmenatation_temp t set t.note = SUBSTRING_INDEX(t.note, '?', 1)
where t.rule_name = 'courses_all_undefined';
-- -----------------------------------------------------------------------------------------------------

drop table if exists ra.visitorsegmentation_temp;

-- 1
create table ra.visitorsegmentation_temp
select distinct
       t.IndvId,
       s.class as eduLevel,
       'member_edex_adobe_com_discussion' as note,
       t.rule_name,
       1 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Discussion d on d.vanityURL = t.note
join edx.MemberSegmentation s on s.memberId = d.createdBy
where t.rule_name = 'member_edex_adobe_com_discussion';

-- 2
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       s.class as eduLevel,
       'member_edex_adobe_com_comment' as note,
       t.rule_name,
       2 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Comment c on c.id = t.note
join edx.MemberSegmentation s on s.memberId = c.createdBy
where t.rule_name = 'member_edex_adobe_com_comment';

alter table ra.visitorsegmentation_temp modify note varchar(80);

-- 3 (MemberSegmentation_all)
insert into ra.visitorsegmentation_temp
select distinct
       t.IndvId,
       s.class as eduLevel,
       'member_edex_adobe_com_discussion' as note,
       t.rule_name,
       3 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Discussion d on d.vanityURL = t.note
join edx.MemberSegmentation_all s on s.memberId = d.createdBy
where t.rule_name = 'member_edex_adobe_com_discussion';

-- 4 (MemberSegmentation_all)
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       s.class as eduLevel,
       'member_edex_adobe_com_comment' as note,
       t.rule_name,
       4 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Comment c on c.id = t.note
join edx.MemberSegmentation_all s on s.memberId = c.createdBy
where t.rule_name = 'member_edex_adobe_com_comment';

-- 5 Seach K12
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       5 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
where t.rule_name = 'search'
and t.eduLevel = 'K12'
-- and t.IndvId not in (select v.IndvId from ra.fullstory_visitor_segmenatation_temp v where v.rule_name = t.rule_name and v.eduLevel = 'HED');
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp v where v.IndvId = t.IndvId and v.rule_name = t.rule_name and v.fiscal_yr_and_per = t.fiscal_yr_and_per and v.eduLevel = 'HED');

-- 6 Seach HED
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       6 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
where t.rule_name = 'search'
and t.eduLevel = 'HED'
-- and t.IndvId not in (select v.IndvId from ra.fullstory_visitor_segmenatation_temp v where v.rule_name = t.rule_name and v.eduLevel = 'K12');
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp v where v.IndvId = t.IndvId and v.rule_name = t.rule_name and v.fiscal_yr_and_per = t.fiscal_yr_and_per and v.eduLevel = 'K12');

-- 7
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       '1' as note,
       t.rule_name,
       7 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
where t.rule_name = 'teaching_resources'
and t.count_partition = 1
and t.eduLevel in ('K12', 'HED');

-- 8
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       '1.1 primary' as note,
       t.rule_name,
       8 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
where t.rule_name = 'teaching_resources'
and t.note = 'primary'
and t.eduLevel = 'K12'
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp t1 where t1.IndvId = t.IndvId and t1.rule_name = t.rule_name and t1.note = 'secondary' and t1.fiscal_yr_and_per = t.fiscal_yr_and_per)
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp s where s.IndvId = t.IndvId and s.rule_name = t.rule_name  and s.note = 'secondary' and s.eduLevel = 'HED' and s.fiscal_yr_and_per = t.fiscal_yr_and_per);

-- 9
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       '1.2 primary' as note,
       t.rule_name,
       9 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
where t.rule_name = 'teaching_resources'
and t.note = 'primary'
and t.eduLevel = 'HED'
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp t1 where t1.IndvId = t.IndvId and t1.rule_name = t.rule_name and t1.note = 'secondary' and t1.fiscal_yr_and_per = t.fiscal_yr_and_per)
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp s where s.IndvId = t.IndvId and s.eduLevel = 'K12' and s.note = t.note and s.rule_name = t.rule_name and s.fiscal_yr_and_per = t.fiscal_yr_and_per);

-- 10
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       '1.3 only primary' as note,
       t.rule_name,
       10 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
where t.rule_name = 'teaching_resources'
and t.note = 'primary'
and t.eduLevel = 'K12'
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp s where s.IndvId = t.IndvId and s.eduLevel = 'HED' and s.note = t.note and s.rule_name = t.rule_name and s.fiscal_yr_and_per = t.fiscal_yr_and_per);

-- 11
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       '1.4 only primary' as note,
       t.rule_name,
       11 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
where t.rule_name = 'teaching_resources'
and t.note = 'primary'
and t.eduLevel = 'HED'
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp s where s.IndvId = t.IndvId and s.eduLevel = 'K12' and s.note = t.note and s.rule_name = t.rule_name and s.fiscal_yr_and_per = t.fiscal_yr_and_per);

-- 12
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       '2 K12' as note,
       t.rule_name,
       12 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
where t.rule_name = 'courses'
and t.eduLevel = 'K12'
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp s where s.IndvId = t.IndvId and s.eduLevel = 'HED' and s.rule_name = t.rule_name and s.fiscal_yr_and_per = t.fiscal_yr_and_per);

-- 13
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       t.eduLevel,
       '2 HED' as note,
       t.rule_name,
       13 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
where t.rule_name = 'courses'
and t.eduLevel = 'HED'
and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp s where s.IndvId = t.IndvId and s.eduLevel = 'K12' and s.rule_name = t.rule_name and s.fiscal_yr_and_per = t.fiscal_yr_and_per);

-- 14 MemberSegmentation
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       s.class as eduLevel,
       'MemberSegmentation_k12' as note,
       t.rule_name,
       14 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Member m on m.vanityUrl = t.note
join edx.MemberSegmentation s on s.memberId = m.id
where t.rule_name = 'member_edex_adobe_com_member';

-- 15 MemberSegmentation_all
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       s.class as eduLevel,
       'MemberSegmentation_all' as note,
       t.rule_name,
       15 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Member m on m.vanityUrl = t.note
join edx.MemberSegmentation_all s on s.memberId = m.id
where t.rule_name = 'member_edex_adobe_com_member';

-- 16 Teaching resources all K12
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       a.eduLevel,
       'teaching_resources_all' as note,
       t.rule_name,
       16 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Resource r on r.vanityUrl = t.note
join edx.ResourceToAcademicLevel l on l.resourceId = r.id 
left join edx.AcademicLevel a on a.id = l.academicLevels
where t.rule_name = 'teaching_resources_all'
and l.layer = 'primary'
and a.eduLevel = 'K12';
-- and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp t1 where t1.IndvId = t.IndvId and t1.rule_name = t.rule_name and t1.fiscal_yr_and_per = t.fiscal_yr_and_per and t1.eduLevel = 'HED');

-- 17 Teaching resources all HED
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       a.eduLevel,
       'teaching_resources_all' as note,
       t.rule_name,
       17 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Resource r on r.vanityUrl = t.note
join edx.ResourceToAcademicLevel l on l.resourceId = r.id 
left join edx.AcademicLevel a on a.id = l.academicLevels
where t.rule_name = 'teaching_resources_all'
and l.layer = 'primary'
and a.eduLevel = 'HED';
-- and not exists (select 1 from ra.fullstory_visitor_segmenatation_temp t1 where t1.rule_name = t.rule_name and t1.fiscal_yr_and_per = t.fiscal_yr_and_per and t1.eduLevel = 'K12');

-- 18 Teaching resources all K12 SEOUrl
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       a.eduLevel,
       'teaching_resources_all' as note,
       t.rule_name,
       18 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Resource r on r.SEOUrl = t.note
join edx.ResourceToAcademicLevel l on l.resourceId = r.id 
left join edx.AcademicLevel a on a.id = l.academicLevels
where t.rule_name = 'teaching_resources_all'
and l.layer = 'primary'
and a.eduLevel = 'K12';

-- 19 Teaching resources all K12 SEOUrl
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       a.eduLevel,
       'teaching_resources_all' as note,
       t.rule_name,
       19 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.Resource r on r.SEOUrl = t.note
join edx.ResourceToAcademicLevel l on l.resourceId = r.id 
left join edx.AcademicLevel a on a.id = l.academicLevels
where t.rule_name = 'teaching_resources_all'
and l.layer = 'primary'
and a.eduLevel = 'HED';

-- 20 Course courses_all_self_paced_course
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       a.eduLevel,
       'courses_all_self_paced_course' as note,
       t.rule_name,
       20 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.course c on c.vanityUrl = t.note
join edx.CourseToAcademicLevel l on l.courseId = c.id
left join edx.AcademicLevel a on a.id = l.academicLevels
where t.rule_name = 'courses_all_self_paced_course';

-- 21 Course courses_all_live_instructor_led_course
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       a.eduLevel,
       'courses_all_live_instructor_led_course' as note,
       t.rule_name,
       21 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.course c on c.vanityUrl = t.note
join edx.CourseToAcademicLevel l on l.courseId = c.id
left join edx.AcademicLevel a on a.id = l.academicLevels
where t.rule_name = 'courses_all_live_instructor_led_course';

-- 22 Course courses_all_instructor_led_course
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       a.eduLevel,
       'courses_all_instructor_led_course' as note,
       t.rule_name,
       22 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.course c on c.vanityUrl = t.note
join edx.CourseToAcademicLevel l on l.courseId = c.id
left join edx.AcademicLevel a on a.id = l.academicLevels
where t.rule_name = 'courses_all_instructor_led_course';

-- 23 Course courses_all_instructor_led_course
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       a.eduLevel,
       'courses_all_toolkit' as note,
       t.rule_name,
       23 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.course c on c.vanityUrl = t.note
join edx.CourseToAcademicLevel l on l.courseId = c.id
left join edx.AcademicLevel a on a.id = l.academicLevels
where t.rule_name = 'courses_all_toolkit';

-- 24 Course courses_all_instructor_led_course
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       a.eduLevel,
       'courses_all_undefined' as note,
       t.rule_name,
       24 as rule_no,
       t.fiscal_yr_and_per,
       t.count,
       t.count_partition
from ra.fullstory_visitor_segmenatation_temp t
join edx.course c on c.vanityUrl = t.note
join edx.CourseToAcademicLevel l on l.courseId = c.id
left join edx.AcademicLevel a on a.id = l.academicLevels
where t.rule_name = 'courses_all_undefined';

-- 25 teaching_resources_all_by_grade, preko reda
insert into ra.visitorsegmentation_temp
select distinct 
       t.IndvId,
       'K12' as eduLevel,
       'th% ad rd% grade' as note,
       'teaching_resources_all_by_grade' as rule_name,
        25 as rule_no,
        t.fiscal_yr_and_per,
        1 as count,
        1 as count_partition
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where (t.pageUrl like '%/teaching-resources/%th-grade%' or t.pageUrl like '%/teaching-resources/%rd-grade%');
-- -----------------------------------------------------------------------------------------------------
/*
-- check US
select count(distinct t.IndvId)
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.segmentName = 'visitors_page_details_us';

-- check ROW
select count(distinct t.IndvId)
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.segmentName = 'visitors_page_details_row';

-- Check 1
select distinct t.pageUrl
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.IndvId not in (select t1.IndvId from ra.visitorsegmentation_temp t1);

-- Check 2
select *
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where 1=1
and not exists (select 1 from ra.visitorsegmentation_temp e where e.IndvId = t.IndvId);
*/
-- -----------------------------------------------------------------------------------------------------

-- create table edx.visitorsegmentation as 

delete from edx.visitorsegmentation;

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 25
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.fiscal_yr_and_per = t.fiscal_yr_and_per and v.eduLevel = 'HED');

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 1
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 2
and t.eduLevel = 'K12'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 2
and t.eduLevel = 'HED'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 3
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 4
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 5
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 6
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 7
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 8
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 9
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 10
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 11
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 12
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 13
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 14
and t.eduLevel = 'K12'
and not exists (select 1 from ra.visitorsegmentation_temp t1 where t1.IndvId = t.IndvId and t1.rule_no = t.rule_no and t1.eduLevel = 'HED' and t1.fiscal_yr_and_per = t.fiscal_yr_and_per)
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 14
and t.eduLevel = 'HED'
and not exists (select 1 from ra.visitorsegmentation_temp t1 where t1.IndvId = t.IndvId and t1.rule_no = t.rule_no and t1.eduLevel = 'K12' and t1.fiscal_yr_and_per = t.fiscal_yr_and_per)
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 15
and t.eduLevel = 'K12'
and not exists (select 1 from ra.visitorsegmentation_temp t1 where t1.IndvId = t.IndvId and t1.rule_no = t.rule_no and t1.eduLevel = 'HED' and t1.fiscal_yr_and_per = t.fiscal_yr_and_per)
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 15
and t.eduLevel = 'HED'
and not exists (select 1 from ra.visitorsegmentation_temp t1 where t1.IndvId = t.IndvId and t1.rule_no = t.rule_no and t1.eduLevel = 'K12' and t1.fiscal_yr_and_per = t.fiscal_yr_and_per)
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 16
and not exists (select 1 from ra.visitorsegmentation_temp t1 where t1.IndvId = t.IndvId and t1.eduLevel = 'HED' and t1.fiscal_yr_and_per = t.fiscal_yr_and_per)
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 17
and not exists (select 1 from ra.visitorsegmentation_temp t1 where t1.IndvId = t.IndvId and t1.eduLevel = 'K12' and t1.fiscal_yr_and_per = t.fiscal_yr_and_per)
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 18
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 19
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 20
and t.eduLevel = 'K12'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'HED' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 20
and t.eduLevel = 'HED'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'K12' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 21
and t.eduLevel = 'K12'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'HED' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 21
and t.eduLevel = 'HED'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'K12' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 22
and t.eduLevel = 'K12'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'HED' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 22
and t.eduLevel = 'HED'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'K12' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 23
and t.eduLevel = 'K12'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'HED' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 23
and t.eduLevel = 'HED'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'K12' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 24
and t.eduLevel = 'K12'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'HED' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);

insert into edx.visitorsegmentation
select distinct 
       t.IndvId,
       t.eduLevel,
       t.note,
       t.rule_name,
       t.rule_no,
       t.fiscal_yr_and_per
from ra.visitorsegmentation_temp t
where t.rule_no = 24
and t.eduLevel = 'HED'
and not exists (select 1 from edx.visitorsegmentation v where v.IndvId = t.IndvId)
and not exists (select 1 from ra.visitorsegmentation_temp v where v.IndvId = t.IndvId and v.rule_no = t.rule_no and v.eduLevel = 'K12' and v.fiscal_yr_and_per = t.fiscal_yr_and_per);
-- -----------------------------------------------------------------------------------------------------

drop table if exists ra.visitorsegmentation_temp_k12;
create table ra.visitorsegmentation_temp_k12 as
select t.IndvId,
	   t.eduLevel,
       t.fiscal_yr_and_per,
       count(*) as count_k12
from ra.visitorsegmentation_temp t
where 1=1
and t.edulevel = 'K12'
and not exists (select 1 from edx.visitorsegmentation t1 where t1.IndvId = t.IndvId)
group by t.IndvId, t.eduLevel, t.fiscal_yr_and_per;

drop table if exists ra.visitorsegmentation_temp_hed;
create table ra.visitorsegmentation_temp_hed as
select t.IndvId,
	   t.eduLevel,
       t.fiscal_yr_and_per,
       count(*) as count_hed
from ra.visitorsegmentation_temp t
where 1=1
and t.edulevel = 'HED'
and not exists (select 1 from edx.visitorsegmentation t1 where t1.IndvId = t.IndvId)
group by t.IndvId, t.eduLevel, t.fiscal_yr_and_per;


insert into edx.visitorsegmentation
select t1.IndvId,
       'K12' as eduLevel,
       'more K12 than HED' as note,
       'more_k12_than_hed' as rule_name,
       90 as rule_no,
       t1.fiscal_yr_and_per
from ra.visitorsegmentation_temp_k12 t1
join ra.visitorsegmentation_temp_hed t2 on t2.IndvId = t1.IndvId
                                             and t2.fiscal_yr_and_per = t1.fiscal_yr_and_per
where t1.count_k12 > t2.count_hed;

insert into edx.visitorsegmentation
select t1.IndvId,
       'HED' as eduLevel,
       'more HED than K12' as note,
       'more_hed_than_k12' as rule_name,
       90 as rule_no,
       t1.fiscal_yr_and_per
from ra.visitorsegmentation_temp_k12 t1
join ra.visitorsegmentation_temp_hed t2 on t2.IndvId = t1.IndvId
                                             and t2.fiscal_yr_and_per = t1.fiscal_yr_and_per
where t1.count_k12 < t2.count_hed;

-- split equal rows to K12 and HED
select round(count(*)/2, 0) into @half_rows
from ra.visitorsegmentation_temp_k12 t1
join ra.visitorsegmentation_temp_hed t2 on t2.IndvId = t1.IndvId
                                             and t2.fiscal_yr_and_per = t1.fiscal_yr_and_per
where t1.count_k12 = t2.count_hed;

insert into edx.visitorsegmentation
select t1.IndvId,
       case when (ROW_NUMBER() OVER (ORDER BY t1.IndvId) > @half_rows) then 'HED' else 'K12' end as eduLevel,
       'half HED half K12' as note,
       'half_hed_half_k12' as rule_name,
       91 as rule_no,
       t1.fiscal_yr_and_per
from ra.visitorsegmentation_temp_k12 t1
join ra.visitorsegmentation_temp_hed t2 on t2.IndvId = t1.IndvId
                                             and t2.fiscal_yr_and_per = t1.fiscal_yr_and_per
where t1.count_k12 = t2.count_hed;
-- -----------------------------------------------------------------------------------------------------


-- prepare final table US
-- dodaj fiscal month
drop table if exists edx.fullstory_visitors_page_details_us_base;
create table edx.fullstory_visitors_page_details_us_base as
select distinct t.IndvId,
                t.fiscal_yr_and_per
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.segmentName = 'visitors_page_details_us';

-- prepare final table ROW
drop table if exists edx.fullstory_visitors_page_details_row_base;
create table edx.fullstory_visitors_page_details_row_base as
select distinct t.IndvId,
                t.fiscal_yr_and_per
from ra.fullstory_tmp_visitors_page_details_fiscal_month t
where t.segmentName = 'visitors_page_details_row';


-- create final table
delete from edx.fullstory_visitors_page_details_fin where fiscal_yr_and_per = @fiscal_month;

-- final table US
-- create table edx.fullstory_visitors_page_details_fin as
insert into edx.fullstory_visitors_page_details_fin
select b.IndvId,
	   b.fiscal_yr_and_per,
       ifnull(s.eduLevel, 'unclassified') as class,
       'US' as country
from edx.fullstory_visitors_page_details_us_base b
left join edx.visitorsegmentation s on s.IndvId = b.IndvId;

-- final table ROW
insert into edx.fullstory_visitors_page_details_fin 
select b.IndvId,
	   b.fiscal_yr_and_per,
       ifnull(s.eduLevel, 'unclassified') as class,
       'ROW' as country
from edx.fullstory_visitors_page_details_row_base b
left join edx.visitorsegmentation s on s.IndvId = b.IndvId;

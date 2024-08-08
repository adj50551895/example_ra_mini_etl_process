-- Date changed: 25.10.2022

-- RULE 1
SELECT m.id as memberId, m.email, m.status, 'K12' as class
FROM ra.MemberEdexDim m
WHERE 1=1 -- m.status = 'active'
AND (UPPER(SUBSTRING(m.email, POSITION('@' IN m.email))) like '%.K12.%' 
     or UPPER(SUBSTRING(m.email, POSITION('@' IN m.email))) like '%.USD.%' 
     or UPPER(SUBSTRING(m.email, POSITION('@' IN m.email))) like '%.ISD.%' 
     or UPPER(m.email) like '%EDU.PS')
AND LOWER(m.email) not like '%student%'
UNION ALL
SELECT m.id as memberId, m.email, m.status, 'HED' as class
FROM ra.MemberEdexDim m
WHERE 1=1 -- m.status = 'active'
AND (UPPER(SUBSTRING(m.email, POSITION('@' IN m.email), LENGTH(m.email))) like '%22.EDU' 
     or UPPER(SUBSTRING(m.email, POSITION('@' IN m.email), LENGTH(m.email))) like '%COLLEGE%' 
     or UPPER(SUBSTRING(m.email, POSITION('@' IN m.email), LENGTH(m.email))) like '%UNIVERSITY%')
AND UPPER(m.email) not like '%student%'
AND UPPER(SUBSTRING(m.email, POSITION('@' IN m.email), LENGTH(m.email))) not like '%EDU.PS'
-- //RULE 1

-- RULE 2

-- RULE 2 Obsolete*
-- create table edx.memberEventResourceTmp as
-- select e.memberId, e.resourceId, a.eduLevel, count(*) as count_
-- from edx.elasticsearchevents_agg e
-- join edx.resourceToAcademicLevel t on t.resourceId = e.resourceId
-- join edx.academicLevel a on a.id = t.academicLevels
-- where 1 =1 
-- -- and e.event like '%resource%'
-- and e.resourceId is not null
-- and e.entityType = 'resource'
-- and e.eventLevel is not null
-- -- Download
-- and (e.event = 'v1.resource.fetched'
-- or e.event = 'resource.click.downloadToDevice'
-- or e.event = 'resource.click.relatedResource'
-- or e.event = 'resource.click.sendToGD'
-- or e.event = 'resource.click.weblink'
-- or e.event = 'v1.resource.export-to-gd-trigger'
-- or e.event = 'v1.resource.export-to-gd-success'
-- or e.event = 'resource.click.sendToOD'
-- or e.event = 'v1.resource.export-to-od-trigger'
-- or e.event = 'v1.resource.export-to-od-success'
-- or e.event = 'resource.click.ccxTemplateLink'
-- or e.event = 'resource.click.ccxTemplateLink.inline'
-- or e.event = 'v1.resource.export-zip-trigger'
-- or e.event = 'v1.resource.export-zip-success'
-- -- Viewed
-- or e.event = 'v1.engaged.resourceLink'
-- or e.event = 'resource.pageview.details'
-- or e.event = 'resource.subview.asset'
-- or e.event = 'resource.click.share'
-- or e.event = 'resource.click.inlineLink'
-- or e.event = 'resource.click.ccxTemplateLink'
-- -- Created
-- or e.event = 'v1.resource.updated'
-- or e.event = 'v1.resource.created'
-- or e.event = 'v1.resource.published')
-- group by e.memberId, e.resourceId, a.eduLevel


drop table if exists ra.memberEduLevelTmp;
create table ra.memberEduLevelTmp as
select distinct e.memberId, a.eduLevel, count(e.memberId) as cnt_memberEduLevel, sum(event_count) as sum_EventCount
from els.elasticsearchevents_agg e	-- els.elasticsearchevents_agg2 e
join edx.resourceToAcademicLevel t on t.resourceId = e.resourceId
join edx.academicLevel a on a.id = t.academicLevels
join edx.Member m on m.id = e.memberId
where 1 =1 
-- and e.event like '%resource%'
and e.resourceId is not null
and e.entityType = 'resource'
and e.eventLevel is not null
and (
-- Download
e.event = 'v1.resource.fetched'
or e.event = 'resource.click.downloadToDevice'
or e.event = 'resource.click.relatedResource'
or e.event = 'resource.click.sendToGD'
or e.event = 'resource.click.weblink'
or e.event = 'v1.resource.export-to-gd-trigger'
or e.event = 'v1.resource.export-to-gd-success'
or e.event = 'resource.click.sendToOD'
or e.event = 'v1.resource.export-to-od-trigger'
or e.event = 'v1.resource.export-to-od-success'
or e.event = 'resource.click.ccxTemplateLink'
or e.event = 'resource.click.ccxTemplateLink.inline'
or e.event = 'v1.resource.export-zip-trigger'
or e.event = 'v1.resource.export-zip-success'
-- Viewed
or e.event = 'v1.engaged.resourceLink'
or e.event = 'resource.pageview.details'
or e.event = 'resource.subview.asset'
or e.event = 'resource.click.share'
or e.event = 'resource.click.inlineLink'
or e.event = 'resource.click.ccxTemplateLink'
-- Created
or e.event = 'v1.resource.updated'
or e.event = 'v1.resource.created'
or e.event = 'v1.resource.published')
group by e.memberId, a.eduLevel;


-- df_resourceK12Only 82738
-- drop table if exists ra.memberSegmentationTempRule2;
-- create table ra.memberSegmentationTempRule2 as 
insert into ra.memberSegmentationTemp
select distinct t.memberId, 'K12' as class_tmp, 'K12' as class, 2 as rule, CURDATE() as createdAt
from ra.memberEduLevelTmp t
where t.eduLevel = 'K12'
and not exists ( 
select 1
from ra.memberEduLevelTmp t2
where t2.memberId = t.memberId
and t2.eduLevel = 'HED'
);

-- df_resourceHEDOnly 28935
insert into ra.memberSegmentationTemp
select distinct t.memberId, 'HED' as class_tmp, 'HED' as class, 2 as rule, CURDATE() as createdAt
from ra.memberEduLevelTmp t
where t.eduLevel = 'HED'
and not exists ( 
select 1
from ra.memberEduLevelTmp t2
where t2.memberId = t.memberId
and t2.eduLevel = 'K12'
);

-- df_possibleK12 215708
insert into ra.memberSegmentationTemp
select t.memberId, 'Possible K12' as class_tmp, 'K12' as class, 2 as rule, CURDATE() as createdAt
-- select t.memberId, t.sum_EventCount, t2.sum_EventCount, t.count_/t2.count_ as "K12/HED Rate2", t.sum_EventCount/t2.sum_EventCount as "K12/HED Rate"
from ra.memberEduLevelTmp t
join ra.memberEduLevelTmp t2 on t2.memberId = t.memberId
where t.eduLevel = 'K12'
and t2.eduLevel = 'HED'
-- and t.cnt_memberEduLevel/t2.cnt_memberEduLevel > 1.5;
and t.sum_EventCount/t2.sum_EventCount > 1.5;
-- and t.count_/t2.count_ > 1.5;

-- df_possibleHED 5349
insert into ra.memberSegmentationTemp
select t.memberId, 'Possible HED' as class_tmp, 'HED' as class, 2 as rule, CURDATE() as createdAt
-- select t.memberId, t.count_,t2.count_, t.count_/t2.count_ as "K12/HED Rate"
from ra.memberEduLevelTmp t
join ra.memberEduLevelTmp t2 on t2.memberId = t.memberId
where t.eduLevel = 'HED'
and t2.eduLevel = 'K12'
-- and t.cnt_memberEduLevel/t2.cnt_memberEduLevel > 1.5;
and t.sum_EventCount/t2.sum_EventCount > 1.5;
-- and t.count_/t2.count_ > 1.5;

-- ============================================================================================================
-- Additional, based on Resource and ResourceToAcademicLevel only
DROP TABLE IF EXISTS ra.tmpResourceAcademicLevel_;
CREATE TABLE ra.tmpResourceAcademicLevel_
select r.createdBy as memberId, a.eduLevel, count(*) as cnt_
from edx.Resource r 
join edx.ResourceToAcademicLevel rl on rl.resourceId = r.id
join edx.AcademicLevel a on a.id = rl.academicLevels
where a.eduLevel in ('K12', 'HED')
-- and r.createdBy = '8248a605-6102-11e2-9a53-12313b016471'
group by r.createdBy, a.eduLevel;


-- 2 K12 only
insert into ra.memberSegmentationTemp
select distinct t1.memberId, 'K12' as class_tmp, 'K12' as class, '2' as rule, CURDATE() as createdAt
from ra.tmpResourceAcademicLevel_ t1
where t1.eduLevel = 'K12'
and t1.memberId not in (select t2.memberId from ra.tmpResourceAcademicLevel_ t2 where t2.eduLevel = 'HED');

-- 3 HED only
insert into ra.memberSegmentationTemp
select distinct t1.memberId, 'HED' as class_tmp, 'HED' as class, '2' as rule, CURDATE() as createdAt
from ra.tmpResourceAcademicLevel_ t1
where t1.eduLevel = 'HED'
and t1.memberId not in (select t2.memberId from ra.tmpResourceAcademicLevel_ t2 where t2.eduLevel = 'K12');

-- 4 more K12 than HED
insert into ra.memberSegmentationTemp
select distinct t1.memberId, 'Possible' as class_tmp, 'K12' as class, '2' as rule, CURDATE() as createdAt
from ra.tmpResourceAcademicLevel_ t1
join ra.tmpResourceAcademicLevel_ t2 on t1.memberId = t2.memberId
where t1.eduLevel = 'K12'
and t2.eduLevel = 'HED'
and t1.cnt_ > t2.cnt_;

-- 5 more HED than K12
insert into ra.memberSegmentationTemp
select distinct t1.memberId, 'Possible HED' as class_tmp, 'HED' as class, '2' as rule, CURDATE() as createdAt
from ra.tmpResourceAcademicLevel_ t1
join ra.tmpResourceAcademicLevel_ t2 on t1.memberId = t2.memberId
where t1.eduLevel = 'HED'
and t2.eduLevel = 'K12'
and t1.cnt_ > t2.cnt_;
-- ============================================================================================================
-- //RULE 2


-- RULE 3
-- 1
CREATE TEMPORARY TABLE ra.tmpMembersAcademicLevel
select t.memberId, a.eduLevel as academicLevel, count(*) as cnt_
from ra.membertoacademicleveledexdim t
join ra.academicleveledexdim a on a.id = t.academicLevels
group by t.memberId, a.eduLevel;

-- 2
DROP TABLE IF EXISTS ra.tmpMembersAcademicLevelTotal;
CREATE TEMPORARY TABLE ra.tmpMembersAcademicLevelTotal
select t.memberId, count(*) as cnt_
from ra.membertoacademicleveledexdim t
join ra.academicleveledexdim a on a.id = t.academicLevels
group by t.memberId;

-- 3
SELECT *
FROM ra.tmpMembersAcademicLevel t1
join ra.tmpMembersAcademicLevelTotal t2 on t1.memberId = t2.memberId 
where t1.academicLevel = 'K12'
and t1.cnt_ = t2.cnt_;

-- 4
SELECT *
FROM ra.tmpMembersAcademicLevel t1
join ra.tmpMembersAcademicLevelTotal t2 on t1.memberId = t2.memberId 
where t1.academicLevel = 'HED'
and t1.cnt_ = t2.cnt_;

-- 5
SELECT *, t1.cnt_/t2.cnt_
FROM ra.tmpMembersAcademicLevel t1
join ra.tmpMembersAcademicLevelTotal t2 on t1.memberId = t2.memberId 
where t1.academicLevel = 'K12'
and (t1.cnt_/t2.cnt_ > 0.5 and t1.cnt_/t2.cnt_ < 1);

-- 6
SELECT *, t1.cnt_/t2.cnt_
FROM ra.tmpMembersAcademicLevel t1
join ra.tmpMembersAcademicLevelTotal t2 on t1.memberId = t2.memberId 
where t1.academicLevel = 'HED'
and (t1.cnt_/t2.cnt_ > 0.5 and t1.cnt_/t2.cnt_ < 1);
-- //RULE 3

-- RULE 4
-- 1
DROP TABLE IF EXISTS ra.tmpMembersToExperienceEduLevelAgg;
CREATE TABLE ra.tmpMembersToExperienceEduLevelAgg
select t.memberId, s.schoolCategory, count(*) as cnt_
from ra.membertoschooltypeedexdim t
join ra.schooltypeedexdim s on s.id = t.schoolTypeID
group by t.memberId, s.schoolCategory

-- 2
DROP TABLE IF EXISTS ra.tmpMembersToExperienceEduLevelAllAgg;
CREATE TABLE ra.tmpMembersToExperienceEduLevelAllAgg
select t.memberId, count(*) as cnt_
from ra.membertoschooltypeedexdim t
join ra.schooltypeedexdim s on s.id = t.schoolTypeID
group by t.memberId

-- 3
select *, t1.cnt_/t2.cnt_
from ra.tmpMembersToExperienceEduLevelAgg t1
join ra.tmpMembersToExperienceEduLevelAllAgg t2 on t2.memberId = t1.memberId
where t1.cnt_ = t2.cnt_
and t1.schoolCategory = 'K12'

-- 4
select *, t1.cnt_/t2.cnt_
from ra.tmpMembersToExperienceEduLevelAgg t1
join ra.tmpMembersToExperienceEduLevelAllAgg t2 on t2.memberId = t1.memberId
where t1.cnt_ = t2.cnt_
and t1.schoolCategory = 'HED'

-- 5
select *, t1.cnt_/t2.cnt_
from ra.tmpMembersToExperienceEduLevelAgg t1
join ra.tmpMembersToExperienceEduLevelAllAgg t2 on t2.memberId = t1.memberId
where t1.schoolCategory = 'K12'
and (t1.cnt_/t2.cnt_ > 0.5 and t1.cnt_/t2.cnt_ < 1)

-- 6
select *, t1.cnt_/t2.cnt_
from ra.tmpMembersToExperienceEduLevelAgg t1
join ra.tmpMembersToExperienceEduLevelAllAgg t2 on t2.memberId = t1.memberId
where t1.schoolCategory = 'HED'
and (t1.cnt_/t2.cnt_ > 0.5 and t1.cnt_/t2.cnt_ < 1)
-- //RULE 4

-- RULE 6
select e.memberID, 'K12' as class_tmp, 'K12' as class, "6" as rule, CURDATE()
from ra.EnrollmentEdExDim e
where e.courseID = '02dd452b-7f08-4774-aae4-cfcb1336acf4'
and e.progress = 100
and e.memberID not in (
	select e1.memberId
	from ra.EnrollmentEdExDim e1
	where e1.courseID = '8e7968d8-914e-4ac1-bc45-23358fec9dfb'
	and e1.progress = 100)
union all
select e.memberID, 'HED' as class_tmp, 'HED' as class, "6" as rule, CURDATE()
from ra.EnrollmentEdExDim e
where e.courseID = '8e7968d8-914e-4ac1-bc45-23358fec9dfb'
and e.progress = 100
and e.memberID not in (
	select e1.memberId
	from ra.EnrollmentEdExDim e1
	where e1.courseID = '02dd452b-7f08-4774-aae4-cfcb1336acf4'
	and e1.progress = 100);

select t.*
from ra.MemberSegmentationTemp t
where t.rule = '6'
-- //RULE 6

-- RULE 7
-- 1
DROP TABLE IF EXISTS ra.tmpCourseToAcademicLevelEduNum;
CREATE TEMPORARY TABLE ra.tmpCourseToAcademicLevelEduNum
select t.courseId, a.eduLevel, count(*) as cnt_
from ra.coursetoacademicleveledexdim t
join ra.academicleveledexdim a on a.id = t.academicLevels
group by t.courseId, a.eduLevel;

-- 2
DROP TABLE IF EXISTS ra.tmpCourseToAcademicLevelEduAllNum;
CREATE TEMPORARY TABLE ra.tmpCourseToAcademicLevelEduAllNum
select t.courseId, count(*) as cnt_
from ra.coursetoacademicleveledexdim t
join ra.academicleveledexdim a on a.id = t.academicLevels
group by t.courseId;

-- 3
DROP TABLE IF EXISTS ra.tmpCourseToAcademicLevelEduFinal;
CREATE TEMPORARY TABLE ra.tmpCourseToAcademicLevelEduFinal
select t1.courseId as courseId1, t1.eduLevel as eduLevel1, t2.courseId as courseId2, t1.cnt_/t2.cnt_ as eduRate
from ra.tmpCourseToAcademicLevelEduNum t1
join ra.tmpCourseToAcademicLevelEduAllNum t2 on t2.courseId = t1.courseId;

-- 4 df_segMemberCourseEnrollment_1
select distinct e.memberId, t.eduLevel1 -- , t.*-- , t.courseId as courseId, t.eduLevel
from ra.EnrollmentEdExDim e
join ra.tmpCourseToAcademicLevelEduFinal t on t.courseId1 = e.courseId
where e.progress = 100
and t.eduRate = 1
and t.eduLevel1 = 'K12';

-- 5
select * -- e.memberId, t.courseId as courseId, t.eduLevel
from ra.EnrollmentEdExDim e
join ra.tmpCourseToAcademicLevelEduFinal t on t.courseId1 = e.courseId
where e.progress = 100
and t.eduRate = 1
and t.eduLevel1 = 'HED';

-- 6 df_segMemberCourseEnrollment_2
select distinct e.memberId, t.eduLevel1 -- -- e.memberId, t.courseId as courseId, t.eduLevel
from ra.EnrollmentEdExDim e
join ra.tmpCourseToAcademicLevelEduFinal t on t.courseId1 = e.courseId
where e.progress = 100
and (t.eduRate > 0.5 and t.eduRate < 1) 
and t.eduLevel1 = 'K12';

-- 7
select * -- e.memberId, t.courseId as courseId, t.eduLevel
from ra.EnrollmentEdExDim e
join ra.tmpCourseToAcademicLevelEduFinal t on t.courseId1 = e.courseId
where e.progress = 100
and (t.eduRate > 0.5 and t.eduRate < 1) 
and t.eduLevel1 = 'HED';

-- RULE 9 ide pod rule 7
-- 1
drop table if exists ra.onlyK12HEDCourse;
create table ra.onlyK12HEDCourse as
select distinct t.courseId, a.eduLevel
from ra.CourseToAcademicLevelEdExDim t
join ra.academicLevelEdExDim a on a.id = t.academicLevels
where 1 =1 -- t.courseId = '02dd452b-7f08-4774-aae4-cfcb1336acf4'
and a.eduLevel = 'K12'
and not exists ( 
select 1
from ra.CourseToAcademicLevelEdExDim t2
join ra.academicLevelEdExDim a2 on a2.id = t2.academicLevels
where t2.courseId = t.courseId
and a2.eduLevel = 'HED'
);

-- 2
insert into ra.onlyK12HEDCourse
select distinct t.courseId, a.eduLevel
from ra.CourseToAcademicLevelEdExDim t
join ra.academicLevelEdExDim a on a.id = t.academicLevels
where 1 =1 -- t.courseId = '02dd452b-7f08-4774-aae4-cfcb1336acf4'
and a.eduLevel = 'HED'
and not exists ( 
select 1
from ra.CourseToAcademicLevelEdExDim t2
join ra.academicLevelEdExDim a2 on a2.id = t2.academicLevels
where t2.courseId = t.courseId
and a2.eduLevel = 'K12'
);

-- 3
insert into ra.memberSegmentationTemp
select distinct e.memberId, 'K12' as class_tmp, 'K12' as class, 9 as rule, CURDATE() as createdAt
from ra.enrollmentedexdim e
join ra.onlyK12HEDCourse t on t.courseId = e.courseId
where 1 =1 -- e.courseID = '51ef2930-bc92-420a-bc2e-d2849775b42f'
and e.memberId not in (
select e2.memberId
from ra.enrollmentedexdim e2 
where e2.courseId not in (select t.courseId from ra.onlyK12HEDCourse  t where t.eduLevel = 'K12'))
-- //RULE 9


-- obsolete
select * -- e.memberId, t.courseId as courseId, t.eduLevel
from ra.EnrollmentEdExDim e
join ra.tmpCourseToAcademicLevelEduFinal t on t.courseId1 = e.courseId
where e.progress = 100
and t.eduLevel1 = 'HED';


select t.*
from ra.MemberSegmentationTemp t
where t.rule = '7'
-- and t.class_tmp = t.class
and t.class_tmp like '%ossible%'
and t.class = 'K12'
-- //RULE 7


-- RULE 8
-- 1
-- all members by course
DROP TABLE IF EXISTS ra.tmpMembersByCourseAll;
CREATE TEMPORARY TABLE ra.tmpMembersByCourseAll
select e.courseId, count(*) as cnt_MembersByCourse
from edx.Enrollment e
group by courseId;

-- 2
-- classified members by course
DROP TABLE IF EXISTS ra.tmpClassifiedMembersByCourseAll;
CREATE TEMPORARY TABLE ra.tmpClassifiedMembersByCourseAll
-- select e.courseId, count(*) as cnt_classifiedMembersByCourse
select e.courseId, count(distinct t.memberId) as cnt_classifiedMembersByCourse
from edx.Enrollment e
join edx.memberSegmentation_temp t on t.memberId = e.memberId
where t.rule != '8' -- 
group by e.courseId;

-- 3
-- courses candidates for rule 8
DROP TABLE IF EXISTS ra.tmpCoursesCandidates;
CREATE TEMPORARY TABLE ra.tmpCoursesCandidates
select t.courseId, t.cnt_MembersByCourse, t2.cnt_classifiedMembersByCourse, t2.cnt_classifiedMembersByCourse/t.cnt_MembersByCourse
from ra.tmpMembersByCourseAll t
join ra.tmpClassifiedMembersByCourseAll t2 on t2.courseId = t.courseId
where t2.cnt_classifiedMembersByCourse/t.cnt_MembersByCourse > 0.5;

-- 4 
-- members by class within course
DROP TABLE IF EXISTS ra.tmpCourseByClass;
CREATE TEMPORARY TABLE ra.tmpCourseByClass as
-- select e.courseId, t.class, count(*) as cnt_courseByClass
select e.courseId, t.class, count(distinct t.memberId) as cnt_courseByClass
from ra.enrollmentedexdim e
join ra.memberSegmentationTemp t on t.memberId = e.memberId
where t.rule != '8' -- 
group by e.courseId, t.class;

-- 5 
-- rate between all members by course and class members by course
DROP TABLE IF EXISTS ra.tmpCourseByClassRate;
CREATE TEMPORARY TABLE ra.tmpCourseByClassRate as
-- select t.courseId, cls.class, cls.cnt_courseByClass, t.cnt_MembersByCourse, cls.cnt_courseByClass / t.cnt_MembersByCourse as rate
select t.courseId, cls.class, cls.cnt_courseByClass, cc.cnt_classifiedMembersByCourse, cls.cnt_courseByClass / cc.cnt_classifiedMembersByCourse as rate
from ra.tmpMembersByCourseAll t
join ra.tmpCourseByClass cls on cls.courseId = t.courseId
join ra.tmpCoursesCandidates cc on cc.courseId = t.courseId
where cls.cnt_courseByClass / cc.cnt_classifiedMembersByCourse > 0.5;

-- 6
insert into ra.memberSegmentationTemp
select distinct e.memberId, t.class as class_tmp, t.class, '8' as rule, CURDATE() as createdAt
from edx.Enrollment e
join ra.tmpCourseByClassRate t on t.courseId = e.courseId
where e.courseId not in (select t.courseId from ra.onlyK12HEDCourse t)
and e.memberId not in (select t.memberId from ra.memberSegmentationTemp t where t.rule != '8') --
-- //RULE 8


-- RULE 9
insert into ra.memberSegmentationTemp
-- 1 K12
select distinct e.memberId, 'K12' as class_tmp, 'K12' as class, 9 as rule, CURDATE() as createdAt
from edx.Enrollment e
where courseId in (
select id
from edx.Course
where academicLevels not like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%')
and e.memberId not in (
select e1.memberId 
from edx.Enrollment e1 
join edx.Course c1 on c1.id = e1.courseId
where c1.academicLevels like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%')
and e.progress >= 10
union all
-- 2 K12
select distinct e.memberId, 'Possible K12' as class_tmp, 'K12' as class, 9 as rule, CURDATE() as createdAt
from edx.Enrollment e
where courseId in (
select id
from edx.Course
where academicLevels not like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%')
and e.memberId not in (
select e1.memberId 
from edx.Enrollment e1 
join edx.Course c1 on c1.id = e1.courseId
where c1.academicLevels like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%')
and e.progress < 10
union all
-- 3 HED
select distinct e.memberId, 'HED' as class_tmp, 'HED' as class, 9 as rule, CURDATE() as createdAt
from edx.Enrollment e
where e.courseId = '8a2ae22b-535c-4a67-8364-9c86d80d3c67'
and e.memberId not in (select e1.memberId from edx.Enrollment e1 where e1.courseId != '8a2ae22b-535c-4a67-8364-9c86d80d3c67')
union
-- 4 HED
select distinct e.memberId, 'Possible HED' as class_tmp, 'HED' as class, 9 as rule, CURDATE() as createdAt
from edx.Enrollment e
where e.courseId = '8a2ae22b-535c-4a67-8364-9c86d80d3c67'
and e.memberId not in (
select e1.memberId 
from edx.Enrollment e1 
join edx.Course c1 on c1.id = e1.courseId
where c1.academicLevels not like '%35bc6c84-f3a5-11ea-9b0b-0e061ef1479f%');
-- // RULE 9


-- RULE 10 Additional (Based on Rule 2 - Only Primary AcademicLevels from Resource)
-- 0, 700082
drop table if exists ra.memberEduLevelPrimaryTmp;
create table ra.memberEduLevelPrimaryTmp as
select distinct e.memberId, a.eduLevel, count(distinct e.memberId) as cnt_memberEduLevel, sum(event_count) as sum_EventCount, min(e.event_date) as min_eventDate, max(e.event_date) as max_eventDate
from els.elasticsearchevents_agg e	-- els.elasticsearchevents_agg2 e
join edx.resourceToAcademicLevel t on t.resourceId = e.resourceId and t.layer = 'primary'
join edx.academicLevel a on a.id = t.academicLevels
join edx.Member m on m.id = e.memberId
where 1 =1 
-- and e.event like '%resource%'
and e.resourceId is not null
and e.entityType = 'resource'
and e.eventLevel is not null
and (
-- Download
e.event = 'v1.resource.fetched'
or e.event = 'resource.click.downloadToDevice'
or e.event = 'resource.click.relatedResource'
or e.event = 'resource.click.sendToGD'
or e.event = 'resource.click.weblink'
or e.event = 'v1.resource.export-to-gd-trigger'
or e.event = 'v1.resource.export-to-gd-success'
or e.event = 'resource.click.sendToOD'
or e.event = 'v1.resource.export-to-od-trigger'
or e.event = 'v1.resource.export-to-od-success'
or e.event = 'resource.click.ccxTemplateLink'
or e.event = 'resource.click.ccxTemplateLink.inline'
or e.event = 'v1.resource.export-zip-trigger'
or e.event = 'v1.resource.export-zip-success'
-- Viewed
or e.event = 'v1.engaged.resourceLink'
or e.event = 'resource.pageview.details'
or e.event = 'resource.subview.asset'
or e.event = 'resource.click.share'
or e.event = 'resource.click.inlineLink'
or e.event = 'resource.click.ccxTemplateLink'
-- Created
or e.event = 'v1.resource.updated'
or e.event = 'v1.resource.created'
or e.event = 'v1.resource.published')
group by e.memberId, a.eduLevel;


-- 1, 19961, 67612
insert into edx.MemberSegmentation_temp
select distinct t.memberId, 'Possible K12' as class_tmp, 'K12' as class, '10' as rule, CURDATE() as createdAt
from ra.memberEduLevelPrimaryTmp t
where 1=1
and t.eduLevel = 'K12'
and not exists ( 
select 1
from ra.memberEduLevelPrimaryTmp t2
where t2.memberId = t.memberId
and t2.eduLevel = 'HED');
-- and t.memberId not in (select m.memberId from edx.MemberSegmentation_temp m);
-- and t.memberId = '0684177a-3dbd-464c-b7a3-000393bf01ae'


-- 2, 12148, 21483
insert into edx.MemberSegmentation_temp
select distinct t.memberId, 'Possible HED' as class_tmp, 'HED' as class, '10' as rule, CURDATE() as createdAt
from ra.memberEduLevelPrimaryTmp t
where t.eduLevel = 'HED'
and not exists ( 
select 1
from ra.memberEduLevelPrimaryTmp t2
where t2.memberId = t.memberId
and t2.eduLevel = 'K12');
-- and t.memberId not in (select m.memberId from edx.MemberSegmentation_temp m);

-- 3, 4630, 14249
insert into edx.MemberSegmentation_temp
select distinct t.memberId, 'Possible K12' as class_tmp, 'K12' as class, '10' as rule, CURDATE() as createdAt
-- select distinct t.memberId, t.sum_EventCount, t2.sum_EventCount, t.sum_EventCount/t2.sum_EventCount as "K12/HED Rate"
from ra.memberEduLevelPrimaryTmp t
join ra.memberEduLevelPrimaryTmp t2 on t2.memberId = t.memberId
where t.eduLevel = 'K12'
and t2.eduLevel = 'HED'
-- and t.memberId not in (select m.memberId from edx.MemberSegmentation_temp m)
-- and t.memberId in (select m.id from ra.tmpMembersToAnalyseN m)
and t.sum_EventCount/t2.sum_EventCount > 1.5;

-- 4, 2360, 4463
insert into edx.MemberSegmentation_temp
select distinct t.memberId, 'Possible HED' as class_tmp, 'HED' as class, '10' as rule, CURDATE() as createdAt
-- select t.memberId, t.sum_EventCount, t2.sum_EventCount, t.sum_EventCount/t2.sum_EventCount as "K12/HED Rate"
from ra.memberEduLevelPrimaryTmp t
join ra.memberEduLevelPrimaryTmp t2 on t2.memberId = t.memberId
where t.eduLevel = 'HED'
and t2.eduLevel = 'K12'
-- and t.memberId not in (select m.memberId from edx.MemberSegmentation_temp m)
and t.sum_EventCount/t2.sum_EventCount > 1.5;

-- ============================================================================================================
-- Additional 10, based on Resource and ResourceToAcademicLevel only
DROP TABLE IF EXISTS ra.tmpResourceAcademicLevelPrimary_;
CREATE TABLE ra.tmpResourceAcademicLevelPrimary_
select r.createdBy as memberId, a.eduLevel, count(*) as cnt_
from edx.Resource r 
join edx.ResourceToAcademicLevel rl on rl.resourceId = r.id
join edx.AcademicLevel a on a.id = rl.academicLevels
where a.eduLevel in ('K12', 'HED')
and rl.layer = 'primary'
group by r.createdBy, a.eduLevel;


-- 2 K12 only
insert into ra.memberSegmentationTemp
select distinct t1.memberId, 'K12' as class_tmp, 'K12' as class, '10' as rule, CURDATE() as createdAt
from ra.tmpResourceAcademicLevelPrimary_ t1
where t1.eduLevel = 'K12'
and t1.memberId not in (select t2.memberId from ra.tmpResourceAcademicLevelPrimary_ t2 where t2.eduLevel = 'HED');

-- 3 HED only
insert into ra.memberSegmentationTemp
select distinct t1.memberId, 'HED' as class_tmp, 'HED' as class, '10' as rule, CURDATE() as createdAt
from ra.tmpResourceAcademicLevelPrimary_ t1
where t1.eduLevel = 'HED'
and t1.memberId not in (select t2.memberId from ra.tmpResourceAcademicLevelPrimary_ t2 where t2.eduLevel = 'K12');

-- 4 more K12 than HED
insert into ra.memberSegmentationTemp
select distinct t1.memberId, 'Possible' as class_tmp, 'K12' as class, '10' as rule, CURDATE() as createdAt
from ra.tmpResourceAcademicLevelPrimary_ t1
join ra.tmpResourceAcademicLevelPrimary_ t2 on t1.memberId = t2.memberId
where t1.eduLevel = 'K12'
and t2.eduLevel = 'HED'
and t1.cnt_ > t2.cnt_;

-- 5 more HED than K12
insert into ra.memberSegmentationTemp
select distinct t1.memberId, 'Possible HED' as class_tmp, 'HED' as class, '10' as rule, CURDATE() as createdAt
from ra.tmpResourceAcademicLevelPrimary_ t1
join ra.tmpResourceAcademicLevelPrimary_ t2 on t1.memberId = t2.memberId
where t1.eduLevel = 'HED'
and t2.eduLevel = 'K12'
and t1.cnt_ > t2.cnt_;
-- ============================================================================================================
-- //RULE 10
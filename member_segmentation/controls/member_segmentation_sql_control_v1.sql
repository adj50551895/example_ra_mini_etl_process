-- 1
select distinct t.rule
from ra.MemberSegmentationTemp t

-- 2
-- 536348, 537585, 538314
select count(distinct t.memberId), m.status
from ra.MemberSegmentationTemp t
join ra.MemberEdexDim m on m.id = t.memberId
where m.isCurrent = 1
group by m.status;

select count(distinct t.memberId)
from ra.MemberSegmentationTemp t;


select *
from ra.MemberSegmentationTemp t
where t.memberId not in (select m.id from ra.MemberEdexDim m);

-- 3
-- 1157224	active
-- 10578	inactive
-- 54995	deleted
-- 9726	banned
select count(*), m.status
from ra.MemberEdexDim m
group by m.status;


-- 4
DROP TABLE IF EXISTS ra.tmpDoubleMembers_;
CREATE TABLE ra.tmpDoubleMembers_ as
select t.memberId, t.class, count(*) as cnt_memberClass
from ra.MemberSegmentationTemp t
where t.memberId in (
select t2.memberId
from ra.MemberSegmentationTemp t2
group by t2.memberId
having count(*)>1)
group by t.memberId, t.class;

-- 5
select *
from ra.tmpDoubleMembers_ t1
join ra.tmpDoubleMembers_ t2 on t1.memberId = t2.memberId
where t1.class = 'K12'
and t2.class = 'HED';

select *
from ra.tmpDoubleMembers_ t1
join ra.tmpDoubleMembers_ t2 on t1.memberId = t2.memberId
where t1.class = 'K12'
and t2.class = 'HED'
and t1.memberId not in (select m.memberId from ra.MemberSegmentationTemp m where m.rule in ('1', '5'))

select *
from ra.MemberSegmentationTemp m
where m.memberId = '0b66a097-390b-47ec-8df4-cf70253cb344'

(https://edex.adobe.com/community/member/c5f68a74)
select *
from ra.MemberSegmentationTemp m
where m.memberId = '00de88f7-74bd-4bd6-a163-9ba1dbfbd8f4'


-- 6
select *
from ra.MemberEdexDim m
where m.status = 'active'
and m.id not in (
select m.memberId
from ra.MemberSegmentationTemp m)

-- 6.1
select *
from ra.tmpDoubleMembers_ t1
join ra.tmpDoubleMembers_ t2 on t1.memberId = t2.memberId
where t1.class = 'K12'
and t2.class = 'HED'
and t1.memberId not in (select m.memberId from ra.MemberSegmentationTemp m where m.rule in ('1', '5'));

-- 7
-- members with activity, potentially for improvement
-- 231638, 230406, 229944, 229907
DROP TEMPORARY TABLE IF EXISTS ra.tmpMembersToAnalyse;
CREATE TEMPORARY TABLE ra.tmpMembersToAnalyse as
select m.id, m.vanityURL
from edx.Member m
where m.status = 'active'
and m.isCurrent = 1
and m.id not in (
select m.memberId
from edx.MemberSegmentation_Temp m)
and exists (
select 1 from els.agg_elasticsearchevents_1 e	--els.elasticsearchevents_agg2 e
where e.memberId = m.id
and year(e.event_date) > 2019);

-- no activity in last 3 years
-- 389234, 389229, 388999
DROP TEMPORARY TABLE IF EXISTS ra.tmpMembersToAnalyseOlder;
CREATE TEMPORARY TABLE ra.tmpMembersToAnalyseOlder as
select m.id, m.vanityURL
from edx.Member m
where m.status = 'active'
and m.isCurrent = 1
and m.id not in (
select m.memberId
from edx.MemberSegmentation_Temp m)
and not exists (
select 1 from els.agg_elasticsearchevents_1 e	-- els.elasticsearchevents_agg2 e
where e.memberId = m.id
and year(e.event_date) > 2019);

-- Unqualified members
select t.id as memberId, t.vanityURl, 'Unqualified' as class,  'No acitvity in 2020, 2021 and 2022' as classDesc
from ra.tmpMembersToAnalyseOlder t
union all
select t.id as memberId, t.vanityURl, 'Unqualified' as class,  '' as classDesc
from ra.tmpMembersToAnalyse t
order by 3, 4 desc;

-- 25.11.2022 Za Increment razlike izmedju membera segmentation
-- 1 (1+2=ukupno increment)
select s.*
from edx.MemberSegmentation s
where s.memberId in (
select t.memberId
from edx.MemberSegmentation_20221124 t  (MemberSegmentation_prev)
where t.memberId = s.memberId
and t.class != s.class
-- and t.rule not in ('1', '5', '8')
)
-- and S.rule not in ('1', '5')
limit 10;
-- 2
select s.*
from edx.MemberSegmentation s
where s.memberId not in (
select t.memberId
from edx.MemberSegmentation_20221124 t);
--

select s.*
from edx.MemberSegmentation_prev s
where s.memberId = 'ffaf2a58-35a2-4d85-87c2-4b7826f10f11' -- '2363e1a3-21fc-4ed4-859e-fad6c1af2ec1'

select s.*
from edx.MemberSegmentation s
where s.memberId = 'ffaf2a58-35a2-4d85-87c2-4b7826f10f11'

select s.*
from edx.MemberSegmentation_temp s
where s.memberId = 'ffaf2a58-35a2-4d85-87c2-4b7826f10f11'
-- //25.11.2022 Za Increment razlike izmedju membera segmentation




-- Classified members
select *
from ra.MemberSegmentation t
order by t.rule, t.class desc;

-- 325709 No activity
-- 194615 With Activity
-- 646532 Segmented members
-- 1166856 Active members
08.11.2022
646532 / 1166856 = 0.5540 -- segmented members
325709 / 1166856 = 0.2791 -- no activity in last 3 years
194615 / 1166856 = 0.1668 -- potentially for improvement


07.11.2022
543170 / 1166856 = 0.4654 -- segmented members
390042 / 1166856 = 0.3342 -- no activity in last 3 years
233644 / 1166856 = 0.2002 -- potentially for improvement



-- Members
1157220	active
10578	inactive
54995	deleted
9726	banned

-- Segmented Members
538047	active
401	banned
11181	deleted
2004	inactive

538314 / 1157220 = 0.4652 -- segmented members
388999 / 1157220 = 0.3361 -- no activity in last 3 years

0.4652 + 0.3361 = 0.801 -- total
229907 / 1157220 = 0.198 -- potentially for improvement


229907 + 388999 + 538314 = 1,157,220



-- 1
538314 + 388999 = 927313
927313 / 1157220 = 0.801

-- 2
1157220 - 388999 = 768221
538314 / 768221 = 0.701

-- 
538314 + 229907 = 768221
768221 / 1157220 = 0.664


-- Photoshop Crash Course doesnt have academic level



-- Hijerarhija
Rule 5:
K12 member - If their GUID matches a known K12 in org ID list
HED member - If their GUID matches a known HED in org ID list

Rule 1:
K12 member - Active members whose email domain contains ‘K12’ or domain includes: ‘USD’, ‘ISD’ or ‘ps’ at the end of their domains.
HED member - Active members whose domain in email address contains ‘.edu’, ‘college’ or ‘university’

Rule 2:
K12 member - If the member has viewed/downloaded/created the Teaching Resource tagged as K12 not HED or has more K12 content viewed/downloaded/created than HED
HED member - If the member has viewed/downloaded/created the Teaching Resource tagged as HED not K12 or has more HED content viewed/downloaded/created than K12

Rule 6:
K12 member - If member completed “Design Your Creative Class (K12)” but not complete in “Design Your Creative Course (HED)” course
HED member - If member completed in “Design Your Creative Course (HED)” but not complete the “Design Your Creative Class (K12)” course

Rule 7:
K12 member - If member enrolls the course which content is tagged as K12 not HED
HED member - If member enrolls the course which content is tagged as HED not K12

RULE 3:
K12 member - If the member has filled "What age level are your students?" in the “Interests" section with K12 tag not HED or has more K12 tags than HED
HED member - If the member has filled "What age level are your students?" in the “Interests" section with HED tag only

RULE 4:
K12 member - If the member's school type in the Experience section is filled with the K12 tag and not the HED or member have more experience in K12 than HED institutions
HED member - If the member's school type in the Experience section is filled with the HED tag and not the K12 or member have more experience in HED than K12 institutions

RULE 8:
If a member is not classified using the previous rules, then as a method for their classification we can use the course enrollment of classified members.
K12 member – If on a certain course, 51% of members are classified as K12, then unclassified members enrolled in the same course can be considered as K12
HED member – If on a certain course, 51% of members are classified as HED, then unclassified members enrolled in the same course can be considered as HED


RULE 7 (Possible) poslednji u hijerarhiji

===========================================================================================================================================================================
*Rule 1
K12 member - Active members whose email domain contains ‘K12’ or domain includes: ‘USD’, ‘ISD’ or ‘ps’ at the end of their domains.
HED member - Active members whose domain in email address contains ‘.edu’, ‘college’ or ‘university’


Rule 2
K12 member - If the member has viewed/downloaded/created the Teaching Resource tagged as K12 not HED or has more K12 content viewed/downloaded/created than HED
HED member - If the member has viewed/downloaded/created the Teaching Resource tagged as HED not K12 or has more HED content viewed/downloaded/created than K12

RULE 3 (Students age level):
K12 member - If the member has filled "What age level are your students?" in the “Interests" section with K12 tag not HED or has more K12 tags than HED
HED member - If the member has filled "What age level are your students?" in the “Interests" section with HED tag only

RULE 4 (School type):
K12 member - If the member's school type in the Experience section is filled with the K12 tag and not the HED or member have more experience in K12 than HED institutions
HED member - If the member's school type in the Experience section is filled with the HED tag and not the K12 or member have more experience in HED than K12 institutions

*RULE 5 (Organization list):
K12 member - If their GUID matches a known K12 in org ID list
HED member - If their GUID matches a known HED in org ID list

*RULE 6 (Course viewed):
K12 member - If member completed “Design Your Creative Class (K12)” but not enrolled in “Design Your Creative Course (HED)” course
HED member - If member completed in “Design Your Creative Course (HED)” but not enrolled the “Design Your Creative Class (K12)” course

*RULE 7 (Course):
K12 member - If member complete the course which content is tagged as K12 not HED
HED member - If member complete the course which content is tagged as HED not K12
*RULE 9 (Courses):
K12 member - If the member has completed the Courses tagged as K12 not HED
K12 member - If the member has completed the Courses tagged as HED not K12

RULE 8 (Course Enrolled):
If a member is not classified using the previous rules, then as a method for their classification we can use the course enrollment of classified members.
K12 member – If on a certain course, e.g. 70% of members are classified as K12, then unclassified members enrolled in the same course can be considered as K12
HED member – If on a certain course, e.g. 70% of members are classified as HED, then unclassified members enrolled in the same course can be considered as HED









select t.rule, count(*), sum(rule), sum(distinct rule)
from edx.memberSegmentation t
group by t.rule
order by t.rule






-- 14.12.2022

-- Unqualified members with no activity in last 3 years
DROP TABLE IF EXISTS ra.tmpMembersToAnalyseOlder;
CREATE TEMPORARY TABLE ra.tmpMembersToAnalyseOlder as
select m.memberId, m.class
from edx.memberSegmentation m
where m.class = 'unqualified'
and not exists (
select 1 from els.elasticsearchevents_agg e	-- els.elasticsearchevents_agg2 e
where e.memberId = m.memberId
and year(e.event_date) > 2019);

-- no activity in last 3 years
select count(*)
from ra.tmpMembersToAnalyseOlder;

-- Unqualified members with not enough data for classification according to defined rules
select count(*)
from edx.memberSegmentation m
where m.class = 'unqualified'
and m.memberId not in (select t1.memberId from ra.tmpMembersToAnalyseOlder t1)



-- 14.12.2022
-- Possible, taking care of possible variance for stats
select t1.desc_ as class, count(*)
from (
select t.classDesc, t.class, 
        case when t.classDesc = '(Possible)' then CONCAT('Possible ', ' ', t.class)
             when t.classDesc = 'Possible' then CONCAT('Possible ', ' ', t.class)
             when t.classDesc = 'Possible K12' then CONCAT('Possible ', ' ', t.class)
             when t.classDesc = 'Possible HED' then CONCAT('Possible ', ' ', t.class)
             else t.class
        end as desc_
from edx.memberSegmentation t
-- where t.class != 'unqualified'
) t1
group by t1.desc_


--02022023
DROP TABLE IF EXISTS ra.tmpMembersToAnalyseOlder;
CREATE TEMPORARY TABLE ra.tmpMembersToAnalyseOlder as
select m.memberId, m.class
from edx.memberSegmentation m
where m.class = 'unclassified'
and not exists (
select 1 from els.agg_elasticsearchevents_1 e	-- els.elasticsearchevents_agg2 e
where e.memberId = m.memberId
and e.event_date > (current_date() - interval 3 year));






--- ------ 03.05.2023
-- tmpMembersToAnalyseOlder
DROP TEMPORARY TABLE IF EXISTS ra.tmpMembersToAnalyseOlder;
CREATE TEMPORARY TABLE ra.tmpMembersToAnalyseOlder as
select m.id, m.vanityURL
from edx.Member m
where m.status = 'active'
and m.isCurrent = 1
and m.id not in (
select m.memberId
from edx.MemberSegmentation_Temp m)
and not exists (
select 1 from els.agg_elasticsearchevents_1 e	-- els.elasticsearchevents_agg2 e
where e.memberId = m.id
and e.event_date > '2020-05-01' -- (current_date() - INTERVAL 3 YEAR) -- tri godine od datuma poslednje segmentacije
-- and year(e.event_date) > 2019
);

-- tmpMembersToAnalyse
DROP TEMPORARY TABLE IF EXISTS ra.tmpMembersToAnalyse;
CREATE TEMPORARY TABLE ra.tmpMembersToAnalyse as
select m.id, m.vanityURL
from edx.Member m
where m.status = 'active'
and m.isCurrent = 1
and m.id not in (select m.memberId from edx.MemberSegmentation_Temp m)
and exists (
    select 1 from els.agg_elasticsearchevents_1 e	-- els.elasticsearchevents_agg2 e
    where e.memberId = m.id
    and e.event_date > '2020-05-01'
-- and year(e.event_date) > 2019
);

delete from ra.tmpMembersToAnalyse where id in (select memberId from edx.MemberSegmentation where rule = '12');
-- //tmpMembersToAnalyse


-- Double Members
DROP TABLE IF EXISTS ra.tmpDoubleMembers_;
CREATE TABLE ra.tmpDoubleMembers_ as
select t.memberId, t.class, count(*) as cnt_memberClass
from edx.MemberSegmentation_temp t
where t.memberId in (
select t2.memberId
from edx.MemberSegmentation_temp t2
group by t2.memberId
having count(*)>1)
group by t.memberId, t.class;


-- 24454
select *
from ra.tmpDoubleMembers_ t1
join ra.tmpDoubleMembers_ t2 on t1.memberId = t2.memberId
where t1.class = 'K12'
and t2.class = 'HED'
and t1.memberId not in (select m.memberId from edx.MemberSegmentation_temp m where m.rule in ('1', '5', '11'))

-- 848228
select distinct memberId
from edx.MemberSegmentation_temp


24454/848228 = 2.9%

-- --------------------------
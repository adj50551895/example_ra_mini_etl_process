delete from edx.MemberSegmentation; 
COMMIT;

-- 1
drop table if exists edx.tmpMemberSegmentationAgg;
create table edx.tmpMemberSegmentationAgg as
select distinct t.memberId, t.class, count(*) as cnt_
from edx.MemberSegmentation_temp t 
join edx.Member m on m.id = t.memberId
where 1=1 -- m.status = 'active'
and m.isCurrent = '1'
group by t.memberId, t.class;
COMMIT;

-- 2
drop table if exists edx.tmpMemberSegmentationBoth;
create table edx.tmpMemberSegmentationBoth as
select t1.memberId, t1.class as class_1, t1.cnt_ as cnt_1, t2.class as class_2, t2.cnt_ as cnt_2
from edx.tmpMemberSegmentationAgg t1
join edx.tmpMemberSegmentationAgg t2 on t1.memberId = t2.memberId
where t1.class = 'K12'
and t2.class = 'HED';
COMMIT;

-- 3
drop table if exists edx.tmpMemberSegmentationRuleAgg;
create table edx.tmpMemberSegmentationRuleAgg as
select t.memberId, t.class, t.rule, count(*) as cnt_
from edx.MemberSegmentation_temp t 
join edx.Member m on m.id = t.memberId
where 1=1 -- m.status = 'active'
and m.isCurrent = '1'
-- and t.rule not in ('5', '1') -- 09.11.2022
group by t.memberId, t.class, t.rule;
COMMIT;

-- Rule 5
insert into edx.MemberSegmentation
select distinct t.memberId, t.class as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
-- and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1) -- Unconditionally
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '5'
and t.class_tmp = '5.1'
and t.class = 'K12';
COMMIT;

insert into edx.MemberSegmentation
select distinct t.memberId, t.class as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
-- and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1) -- Unconditionally
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '5'
and t.class_tmp = '5.1'
and t.class = 'HED';
COMMIT;

insert into edx.MemberSegmentation
select distinct t.memberId, t.class as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
-- and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1) -- Unconditionally
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '5'
and t.class_tmp = '5.2'
and t.class = 'K12';
COMMIT;

insert into edx.MemberSegmentation
select distinct t.memberId, t.class as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
-- and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1) -- Unconditionally
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '5'
and t.class_tmp = '5.2'
and t.class = 'HED';
COMMIT;

-- Rule 11 -- 24.01.2023, domain table before rule 1
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
-- and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1) -- Unconditionally
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '11';
COMMIT;

-- Rule 1
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
-- and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1) -- Unconditionally
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '1';
COMMIT;

-- Rule 2, has possible
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp not like 'Possible%'
and t.rule = '2';
COMMIT;

-- Rule 6
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '6';
COMMIT;

-- Rule 7
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp not like 'Possible%'
and t.rule = '7';
COMMIT;

-- Rule 3
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp not like 'Possible%'
and t.rule = '3';
COMMIT;

-- Rule 4
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp not like 'Possible%'
and t.rule = '4';
COMMIT;

-- Rule 8
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp not like 'Possible%'
and t.rule = '8';
COMMIT;

-- Rule 9
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class as class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp not like 'Possible%'
and t.rule = '9';

-- ==========================================================================================================
-- Rules that have Possible part - Rules 3, 4  7, 2, 9
-- ==========================================================================================================
-- Rule 3
insert into edx.MemberSegmentation
select distinct t.memberId, 'Possible' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp like 'Possible%'
and t.rule = '3';
COMMIT;

-- Rule 4
insert into edx.MemberSegmentation
select distinct t.memberId, 'Possible' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp like 'Possible%'
and t.rule = '4';
COMMIT;

-- Rule 7, Needs to be last
insert into edx.MemberSegmentation
select distinct t.memberId, 'Possible' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp like 'Possible%'
and t.rule = '7';
COMMIT;

-- Rule 9
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class as class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp like 'Possible%'
and t.rule = '9';


-- Rule 2
insert into edx.MemberSegmentation
select distinct t.memberId, 'Possible' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1)
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.class_tmp like 'Possible%'
and t.rule = '2';
COMMIT;

-- ==========================================================================================================
-- Both K12 and HED Segmentation where  count K12 != count HED
-- ==========================================================================================================
-- Rule 6
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_1 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 > t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '6';
COMMIT;

-- HED 25
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_2 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 < t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '6';
COMMIT;

-- Rule 3
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_1 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 > t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '3';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_2 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 < t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '3';
COMMIT;

-- Rule 4
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_1 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 > t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '4';
COMMIT;

-- HED 0
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_2 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 < t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '4';
COMMIT;

-- Rule 8
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_1 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 > t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '8';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_2 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 < t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '8';
COMMIT;

-- Rule 7
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_1 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 > t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '7';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_1 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 < t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '7';
COMMIT;


-- Rule 9
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_1 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 > t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '9';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_1 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 < t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '9';
COMMIT;



-- Rule 2
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_1 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 > t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '2';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t1.class_2 as class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.tmpMemberSegmentationBoth t1 on t1.memberId = t.memberId
join edx.Member m on m.id = t.memberId
where m.status = 'active'
and t1.cnt_1 < t1.cnt_2
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.rule = '2';
COMMIT;

-- ==========================================================================================================
-- Both K12 and HED Segmentation where count K12 = count HED, No Rule 5 and Rule 1
-- ==========================================================================================================
-- Rule 6
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'K12'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '6';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'HED'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '6';
COMMIT;

-- Rule 7
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'K12'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '7';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'HED'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '7';
COMMIT;

-- Rule 3
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'K12'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '3';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'HED'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '3';
COMMIT;

-- Rule 4
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'K12'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '4';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'HED'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '4';
COMMIT;

-- Rule 8
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'K12'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '8';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'HED'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '8';
COMMIT;

-- Rule 9
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'K12'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '9';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'HED'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '9';
COMMIT;


-- Rule 2
-- K12
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'K12'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '2';
COMMIT;

-- HED
insert into edx.MemberSegmentation
select distinct t.memberId, '(Possible)' as classDesc, t.class, t.rule, CURDATE() as createdAt
from edx.tmpMemberSegmentationRuleAgg t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.class = 'HED'
and t.memberID not in (select memberId from edx.MemberSegmentation)
and t.memberID in (select memberId from edx.tmpMemberSegmentationBoth)
and t.rule = '2';
COMMIT;

-- ==========================================================================================================
-- RULE 10 Additional - Based on Rule 2, Only Primary AcademicLevels from Resource
-- ==========================================================================================================
insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class as class, 10 as rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.Member m on m.id = t.memberId
where t.rule = '10'
and m.status = 'active'
and t.memberId not in (select m1.memberId from edx.MemberSegmentation m1)
and t.class = 'K12'
and t.class_tmp not like '%ossible%';

insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class as class, 10 as rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.Member m on m.id = t.memberId
where t.rule = '10'
and m.status = 'active'
and t.memberId not in (select m1.memberId from edx.MemberSegmentation m1)
and t.class = 'K12'
and t.class_tmp like '%ossible%';

insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class as class, 10 as rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.Member m on m.id = t.memberId
where t.rule = '10'
and m.status = 'active'
and t.memberId not in (select m1.memberId from edx.MemberSegmentation m1)
and t.class = 'HED'
and t.class_tmp not like '%ossible%';

insert into edx.MemberSegmentation
select distinct t.memberId, t.class_tmp as classDesc, t.class as class, 10 as rule, CURDATE() as createdAt
from edx.MemberSegmentation_temp t
join edx.Member m on m.id = t.memberId
where t.rule = '10'
and m.status = 'active'
and t.memberId not in (select m1.memberId from edx.MemberSegmentation m1)
and t.class = 'HED'
and t.class_tmp like '%ossible%';
-- ==========================================================================================================
-- //RULE 10 (Based on Rule 2, Only Primary AcademicLevels from Resource)
-- ==========================================================================================================


-- ==========================================================================================================
-- Insert active unclassified, unqualified members
-- ==========================================================================================================
insert into edx.MemberSegmentation
SELECT m.id  as memberId, "" as classDesc, "unclassified" as class, "" as rule, CURDATE() as createdAt
FROM edx.Member m
WHERE m.id not in (select t.memberId from edx.MemberSegmentation t)
AND m.status = 'active';
-- ==========================================================================================================
-- //Insert active unqualified members
-- ==========================================================================================================


-- ==========================================================================================================
-- RULE 12. Unclassified members with activity in last 3 years, with country code US set as K12
-- ==========================================================================================================
-- Unclassified members with activity in last 3 years
DROP TABLE IF EXISTS ra.tmp_membersegmentation_with_activity_in_3yrs;
CREATE TEMPORARY TABLE ra.tmp_membersegmentation_with_activity_in_3yrs as
select s.memberId, 'K12' as class, m.countryCode
from edx.MemberSegmentation s
join edx.Member m on m.id = s.memberId
where s.class = 'unclassified'
and (m.countryCode = 'US' or s.memberId in (select l.memberId from edx.ud_lookup l))
and exists (
select 1 from els.agg_elasticsearchevents_1 e
where e.memberId = s.memberId
and e.event_date > current_date() - INTERVAL 3 YEAR);

-- Update
update edx.MemberSegmentation set classDesc = 'possible K12', class = 'K12', rule = '12'
where memberId in (select t.memberId from ra.tmp_membersegmentation_with_activity_in_3yrs t);
-- ==========================================================================================================
-- //RULE 12. Unclassified members with activity in last 3 years, with country code US set as K12
-- ==========================================================================================================


-- ==========================================================================================================
-- Insert to increment table
-- ==========================================================================================================
-- delete from edx.MemberSegmentation_incr;

-- insert edx.MemberSegmentation_incr
-- SELECT t1.memberId, t1.classDesc, t1.class, t1.rule, t1.createdAt
-- FROM edx.MemberSegmentation AS t1 
-- LEFT JOIN edx.MemberSegmentation_prev AS t2 ON t1.memberId=t2.memberId AND t1.class=t2.class 
-- WHERE t2.memberId IS NULL;
-- ==========================================================================================================
-- //Insert to increment table
-- ==========================================================================================================


-- ==========================================================================================================
-- Insert to membersegmenttion_all table 22.05.2023
-- ==========================================================================================================
insert into edx.membersegmentation_backup_all
select t.memberId,
       t.classDesc,
       t.class,
       t.rule,
       date(t.createdAt) as createdAt
from edx.MemberSegmentation t;

-- 01.08.2023
delete from edx.membersegmentation_all where class = 'unclassified';

delete from edx.membersegmentation_all t
where t.memberId in (select t1.memberId from edx.membersegmentation t1 where t1.memberID = t.memberId and t1.class != t.class and t1.class != 'unclassified');

insert into edx.membersegmentation_all 
select s.*
from edx.membersegmentation s
where s.memberId not in (select t1.memberId from edx.membersegmentation_all t1);
-- //01.08.2023
-- ==========================================================================================================
-- //Insert to membersegmenttion_all table
-- ==========================================================================================================


-- ==========================================================================================================
-- Drop temp tables 24.04.2023
-- ==========================================================================================================
drop table if exists edx.tmpMemberSegmentationAgg;
drop table if exists edx.tmpMemberSegmentationBoth;
drop table if exists edx.tmpMemberSegmentationRuleAgg;
-- ==========================================================================================================
-- //Drop temp tables
-- ==========================================================================================================

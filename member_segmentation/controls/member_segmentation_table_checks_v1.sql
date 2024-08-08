-- ==========================================================================================================
-- 1
select t.rule, t.class, count(*)
from edx.MemberSegmentation t
group by t.rule, t.class
order by 1, 2;

select t.rule, t.class, count(*)
from ex_application.MemberSegmentation t
group by t.rule, t.class
order by 1, 2;

-- 2
select t.rule, t.class, count(*)
from ex_application.MemberSegmentation t
group by t.rule, t.class
order by 1, 2;

select t.rule, t.class, count(*)
from ex_application.EdExMemberSegmentation t
group by t.rule, t.class
order by 1, 2;



-- 1 additional
select t.rule, count(*)
from edx.MemberSegmentation t
group by t.rule
order by t.rule;

select t.rule, count(*)
from ex_application.MemberSegmentation t
group by t.rule
order by t.rule;


-- 2
select t.class, count(*)
from edx.MemberSegmentation t
group by t.class;

select t.class, count(*)
from ra.MemberSegmentation_20230124 t
group by t.class;


-- 3
select t.class, count(*)
from edx.MemberSegmentation t
group by t.class;

select t.class, count(*)
from ex_application.MemberSegmentation t
group by t.class;

-- wich rules have Possible% 
select distinct t.rule, t.class_tmp
from edx.MemberSegmentation_temp t;

select distinct t.*
from edx.MemberSegmentation_temp t
join edx.member m on m.id = t.memberId
where m.status = 'active'
and t.memberId not in (select t1.memberId from edx.MemberSegmentation t1)
and t.memberId not in (select t1.memberId from edx.tmpMemberSegmentationBoth t1);

select count(*)
from edx.tmpMemberSegmentationBoth t;

select count(*)
from edx.MemberSegmentation t; 

select distinct t3.rule, count(*)
from edx.memberSegmentation t3
where t3.memberid in (
	select t1.memberId -- , t1.class
	from (
		select t.memberId, t.class
		from ra.MemberSegmentation_20230104 t
		where t.memberId in (select memberId from edx.MemberSegmentation)
		union all
		select t.memberId, t.class
		from edx.MemberSegmentation t
		) t1
	group by t1.memberId, t1.class
	having count(*) = 1)
and t3.memberId not in (select t4.memberId from edx.tmpMemberSegmentationBoth t4)
group by t3.rule;
 -- ==========================================================================================================

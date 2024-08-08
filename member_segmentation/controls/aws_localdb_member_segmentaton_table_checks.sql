-- AWS
select t.class, t.rule, count(*)
from ex_application.MemberSegmentation t
group by t.class, t.rule
order by 1,2,3;

select t.class, count(*)
from ex_application.MemberSegmentation t
group by t.class
order by 1,2;

-- Local
select t.class, t.rule, count(*)
from edx.MemberSegmentation t
group by t.class, t.rule
order by 1,2,3;

select t.class, t.rule, count(*)
from ra.MemberSegmentation_20230117 t
group by t.class, t.rule
order by 1,2,3;

select t.class, count(*)
from edx.MemberSegmentation t
group by t.class
order by 1,2;



-- 
-- delete from ex_application.MemberSegmentation;
insert into ex_application.MemberSegmentation 
select t.* from ex_application.MemberSegmentation_backup t;
delete from ex_application.MemberSegmentation_hist
where date...;


--
select t.*, s.*
from ex_application.EdExMemberSegmentation t
join ex_application.MemberSegmentation s on s.memberId = t.memberId
where s.class != t.class;

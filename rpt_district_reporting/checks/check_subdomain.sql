drop table if exists ra.tmp_member_district_domain_to_check;
create table ra.tmp_member_district_domain_to_check as
SELECT m.id as memberId,
       lower(m.email) as email,
       trim(lower(substring(m.email, position('@' in m.email)+1, length(m.email)))) as email_domain
       -- ,count(*)
FROM edx.Member m
WHERE EXISTS (
    SELECT 1
    FROM edx.district_domains d
    WHERE 1=1
    and lower(m.email) COLLATE utf8mb4_general_ci LIKE CONCAT("%.", d.domain) -- , '%'
);

create temporary table ra.tmp_member_district_domain_to_insert as
select distinct m.email_domain, 
                substring(m.email_domain, position('.' in m.email_domain)+1, length(m.email_domain)) as domain
from ra.tmp_member_district_domain_to_check m
where m.email_domain not in (
select d.domain COLLATE utf8mb4_general_ci
from edx.district_domains d
);

-- insert into edx.district_domains
select distinct
       d.district_id,
       d.school_district_name,
       t.email_domain,
       d.sky_scraper,
       d.top_500,
       d.top_200,
       d.top_100,
       d.pod,
       d.nces_id,
       d.vsky,
       d.high_rise,
       d.domain_class,
       d.priority_institution
from ra.tmp_member_district_domain_to_insert t
join edx.district_domains d on d.domain COLLATE utf8mb4_general_ci = t.domain
order by 1;

drop table if exists  ra.tmp_member_district_domain_to_check;
-- ------

select *
from edx.district_domains d
where d.domain like '%bba10.mccombs.utexas.edu'

select *
from edx.Member m
where trim(lower(substring(m.email, position('@' in m.email)+1, length(m.email)))) like 'bba10.mccombs.utexas.edu'
select -- t.district_id,
       t.school_district_name as "School District Name",
       t.edex_members as "EdEx Members",
       t.members_acquired_content as "Members that Acquired Content",
       t.acquisition_rate as "Acquisition Rate",
       t.edex_driven_first_time_logins as "EdEx Driven First Time Logins",
       -- t.student_first_time_logins as "Student First Time Logins",
       t.first_time_login_rate as "First Time Login Rate"
from edx.rpt_district_edex_activity_fin t
where t.isCurrent = 1
and exists (select 1 from edx.district_domains d where d.district_id = t.district_id and d.domain_class = 'K12')
order by 1,2;
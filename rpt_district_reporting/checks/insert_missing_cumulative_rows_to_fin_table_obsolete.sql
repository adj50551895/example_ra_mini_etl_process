-- 0
create table edx.rpt_district_edex_activity_monthly_fin_backup_20230901 as
select *
from edx.rpt_district_edex_activity_monthly_fin t;

-- 1 
create temporary table ra.tmp_fiscal_yr_and_per as
select distinct dd.fiscal_yr_and_per
from edx.hana_dim_date dd
where dd.fiscal_yr_and_per > 202012 and 202309;

-- 2
create temporary table ra.tmp_fiscal_yr_and_per_district as
SELECT distinct d.fiscal_yr_and_per,
       t.district_id
FROM ra.tmp_fiscal_yr_and_per d
CROSS JOIN edx.rpt_district_edex_activity_monthly_fin t;

-- 3 identify missing row for insert
drop temporary table if exists ra.tmp_fiscal_yr_and_per_district_to_insert;
create temporary table ra.tmp_fiscal_yr_and_per_district_to_insert as
select distinct d.fiscal_yr_and_per,
                d.district_id
                -- ,t.district_id
                -- ,DATE_FORMAT(DATE_SUB(date(concat(d.fiscal_yr_and_per,'01')), interval 1 month),'%Y%m') AS prev_fiscal_yr_and_per
from ra.tmp_fiscal_yr_and_per_district d
left join edx.rpt_district_edex_activity_monthly_fin t on t.fiscal_yr_and_per = d.fiscal_yr_and_per
                                                      and t.district_id = d.district_id
where t.district_id is null
and d.fiscal_yr_and_per >= (select min(t1.fiscal_yr_and_per) from edx.rpt_district_edex_activity_monthly_fin t1 where t1.district_id = d.district_id);

-- 4 create table for insert
create table ra.rpt_district_edex_activity_monthly_fin_insert as
SELECT districts.district_id,
       districts.school_district_name,
       calendar.fiscal_yr_and_per,
       districts.sky_scraper,
       districts.top_500,
       districts.top_200,
       districts.top_100,
       districts.pod,
       0 as a1,
       COALESCE(s.edex_members_by_month_cum, (SELECT edex_members_by_month_cum FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS edex_members_by_month_cum,
       0 as a2,
       0 as a3,
       COALESCE(s.cum_acquisition_by_members, (SELECT cum_acquisition_by_members FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS cum_acquisition_by_members,
       COALESCE(s.rate_content_acquisition_by_members, (SELECT rate_content_acquisition_by_members FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS rate_content_acquisition_by_members,
       0 as a4,
       0 as a5,
       0 as a6,
       0 as a7,
       0 as a8,
       0 as a9,
       COALESCE(s.cum_express_correlation_logins, (SELECT cum_express_correlation_logins FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS cum_express_correlation_logins,
       COALESCE(s.rate_correlated_express_logins, (SELECT rate_correlated_express_logins FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS rate_correlated_express_logins,
       0 as a10,
       0 as a11,
       COALESCE(s.cum_express_causation_logins, (SELECT cum_express_causation_logins FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS cum_express_causation_logins,
       COALESCE(s.rate_causation_express_logins, (SELECT rate_causation_express_logins FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS rate_causation_express_logins,
       0 as a12,
       0 as a13,
       COALESCE(s.cum_cc_correlation_logins, (SELECT cum_cc_correlation_logins FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS cum_cc_correlation_logins,
       COALESCE(s.rate_cc_correlation_logins, (SELECT rate_cc_correlation_logins FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS rate_cc_correlation_logins,
       0 as a14,
       0 as a15,
       COALESCE(s.cum_cc_causation_logins, (SELECT cum_cc_causation_logins FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS cum_cc_causation_logins,
       COALESCE(s.rate_cc_causation_logins, (SELECT rate_cc_causation_logins FROM edx.rpt_district_edex_activity_monthly_fin WHERE district_id = districts.district_id AND fiscal_yr_and_per < calendar.fiscal_yr_and_per ORDER BY fiscal_yr_and_per DESC LIMIT 1)) AS rate_cc_causation_logins,
       0 as a16,
       0 as a17,
       current_date() as createdAt,       
       0 as isCurrent
FROM
    (SELECT DISTINCT fiscal_yr_and_per, concat(left(fiscal_yr_and_per, 4), '-', right(fiscal_yr_and_per, 2)) as Fiscal_Month FROM edx.rpt_district_edex_activity_monthly_fin) calendar
CROSS JOIN
    (SELECT DISTINCT district_id, 
                     school_district_name,
                     sky_scraper,
                     top_500,
                     top_200,
                     top_100,
                     pod   
	 FROM edx.rpt_district_edex_activity_monthly_fin) districts
LEFT JOIN
    edx.rpt_district_edex_activity_monthly_fin s
ON
    calendar.fiscal_yr_and_per = s.fiscal_yr_and_per AND districts.district_id = s.district_id;

-- insert to final table
insert into edx.rpt_district_edex_activity_monthly
select t.*
from ra.rpt_district_edex_activity_monthly_fin_insert t
where t.fiscal_yr_and_per in (select b.fiscal_yr_and_per from ra.tmp_fiscal_yr_and_per_district_to_insert b where b.district_id = t.district_id)
and not exists (select 1 from edx.rpt_district_edex_activity_monthly_fin t1 where t1.district_id = t.district_id and t1.fiscal_yr_and_per = t.fiscal_yr_and_per)
order by 3, 1;


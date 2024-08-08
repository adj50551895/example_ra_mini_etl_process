-- 0
-- delete from edx.rpt_district_edex_activity_monthly;
-- insert into edx.rpt_district_edex_activity_monthly select * from edx.rpt_district_edex_activity_monthly_backup_20230901;

-- 1
select DATE_FORMAT(DATE_SUB(date(concat(max(t.fiscal_yr_and_per),'01')), interval 2 month),'%Y%m'), max(t.fiscal_yr_and_per)
into @m_fiscal_yr_and_per_start, @m_fiscal_yr_and_per_end
from edx.rpt_district_edex_activity_monthly t;

-- 2
drop temporary table if exists ra.tmp_fiscal_yr_and_per_district;
create temporary table ra.tmp_fiscal_yr_and_per_district as
SELECT distinct d.fiscal_yr_and_per,
       t.district_id
FROM edx.hana_dim_date d
CROSS JOIN edx.rpt_district_edex_activity_monthly t
where d.fiscal_yr_and_per between @m_fiscal_yr_and_per_start and @m_fiscal_yr_and_per_end
and t.isCurrent = 1;

-- 3
drop table edx.rpt_district_edex_activity_monthly_1;
create table edx.rpt_district_edex_activity_monthly_1 as
select *
from edx.rpt_district_edex_activity_monthly
where isCurrent = 1;

-- 3 identify missing rows for insert
drop table if exists ra.tmp_fiscal_yr_and_per_district_to_insert;
create table ra.tmp_fiscal_yr_and_per_district_to_insert as
select distinct d.fiscal_yr_and_per,
                d.district_id,
                -- t.district_id as t_district_id,
                (select max(b1.fiscal_yr_and_per) from edx.rpt_district_edex_activity_monthly_1 b1 where b1.district_id = d.district_id and b1.fiscal_yr_and_per < d.fiscal_yr_and_per) as prev_fiscal_yr_and_per
from ra.tmp_fiscal_yr_and_per_district d
left join edx.rpt_district_edex_activity_monthly_1 t on t.fiscal_yr_and_per = d.fiscal_yr_and_per
                                                    and t.district_id = d.district_id
where 1=1
and t.district_id is null
and d.fiscal_yr_and_per >= (select min(t1.fiscal_yr_and_per) from edx.rpt_district_edex_activity_monthly_1 t1 where t1.district_id = d.district_id);

-- 4 insert missing vlaues to the monthly table
-- insert into edx.rpt_district_edex_activity_monthly
select  t.district_id, 
        t.school_district_name,
        s.fiscal_yr_and_per,
        t.sky_scraper,
        t.top_500,
        t.top_200,
        t.top_100,
        t.pod,
        0 as edex_members_by_month,
        t.edex_members_by_month_cum,
        0 as visits_by_month,
        0 as overall_visits,
        t.cum_acquisition_by_members,
        t.rate_content_acquisition_by_members,
        0 as acquisition_by_month,
        0 as rate_content_acquisition_by_month,
        0 as first_time_acquisition_by_month,
        0 as rate_first_content_acquisition_by_members,
        0 as repeat_acquisition_by_month,
        0 as rate_repeat_content_acquisition_by_members,
        t.cum_express_correlation_logins,
        t.rate_correlated_express_logins,
        0 as express_correlation_logins,
        0 as rate_correlated_express_logins_monthly,
        t.cum_express_causation_logins,
        t.rate_causation_express_logins,
        0 as express_causation_logins,
        0 as rate_causation_express_logins_monthly,
        t.cum_cc_correlation_logins,
        t.rate_cc_correlation_logins,
        0 as cc_correlation_logins,
        0 as rate_cc_correlation_logins_monthly,
        t.cum_cc_causation_logins,
        t.rate_cc_causation_logins,
        0 as cc_causation_logins,
        0 as rate_cc_causation_logins_monthly,
        t.createdAt, 
        1 as isCurrent
from ra.tmp_fiscal_yr_and_per_district_to_insert s
join edx.rpt_district_edex_activity_monthly_1 t on t.fiscal_yr_and_per = s.prev_fiscal_yr_and_per
                                               and t.district_id = s.district_id;

-- drop tables
drop table edx.rpt_district_edex_activity_monthly_1;
drop table if exists ra.tmp_fiscal_yr_and_per_district_to_insert;

/*
select *
from edx.rpt_district_edex_activity_monthly
where iscurrent = 1 
and district_id = 69;
*/



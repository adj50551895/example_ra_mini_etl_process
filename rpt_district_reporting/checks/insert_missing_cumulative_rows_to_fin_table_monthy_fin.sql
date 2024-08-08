-- 1
drop temporary table if exists ra.tmp_fiscal_yr_and_per_district;
create temporary table ra.tmp_fiscal_yr_and_per_district as
SELECT distinct d.fiscal_yr_and_per,
                t.district_id
FROM edx.hana_dim_date d
CROSS JOIN edx.rpt_district_edex_activity_monthly_fin t
where d.fiscal_yr_and_per between 202011 and 202309;


-- 2 identify missing rows for insert
drop temporary table if exists ra.tmp_fiscal_yr_and_per_district_to_insert;
create temporary table ra.tmp_fiscal_yr_and_per_district_to_insert as
select distinct d.fiscal_yr_and_per,
                d.district_id,
                -- t.district_id,
                (select max(b1.fiscal_yr_and_per) from edx.rpt_district_edex_activity_monthly_fin b1 where b1.district_id = d.district_id and b1.fiscal_yr_and_per < d.fiscal_yr_and_per) as prev_fiscal_yr_and_per
from ra.tmp_fiscal_yr_and_per_district d
left join edx.rpt_district_edex_activity_monthly_fin t on t.fiscal_yr_and_per = d.fiscal_yr_and_per
                                                      and t.district_id = d.district_id
where 1=1
and t.district_id is null
and d.fiscal_yr_and_per >= (select min(t1.fiscal_yr_and_per) from edx.rpt_district_edex_activity_monthly_fin t1 where t1.district_id = d.district_id);

-- 3 insert missings to the fin table
insert into edx.rpt_district_edex_activity_monthly_fin
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
        t.isCurrent
from ra.tmp_fiscal_yr_and_per_district_to_insert s
join edx.rpt_district_edex_activity_monthly_fin t on t.fiscal_yr_and_per = s.prev_fiscal_yr_and_per
                                                 and t.district_id = s.district_id;


-- select t.*
-- from edx.rpt_district_edex_activity_monthly_fin t
-- where t.district_id = 80;
/*
-- check
select  t.fiscal_yr_and_per,
        t.district_id,
        count(*)
from edx.rpt_district_edex_activity_monthly_fin t
group by t.fiscal_yr_and_per,
         t.district_id
having count(*) > 1;
*/
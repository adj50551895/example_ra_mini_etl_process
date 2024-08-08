select -- t.fiscal_yr_and_per,
       concat(left(t.fiscal_yr_and_per, 4), '-', right(t.fiscal_yr_and_per, 2)) as "Fiscal Month",
       -- dd.fiscal_yr_and_per_desc as "Fiscal Month",
       DATE_FORMAT(DATE_SUB(DATE(CONCAT(left(t.fiscal_yr_and_per, 4), '-', right(t.fiscal_yr_and_per, 2), '-01')), INTERVAL 1 MONTH), '%Y-%m') as "Calendar Month",
       t.school_district_name as "District",
       case when t.sky_scraper = 1 then 'Sky Scraper'
            else 'Non-Sky Scraper'
       end as "Sky Scraper District",
       case when t.top_500 = 1 then 'Top 500'
            else 'Non-Top 500'
       end as "Top 500 Districts",
       case when t.top_200 = 1 then 'Top 200'
            else 'Non-Top 200'
       end as "Top 200 Districts",
       case when t.top_100 = 1 then 'Top 100'
            else 'Non-Top 100'
       end as "Top 100 Districts",
       -- t.sky_scraper as "Sky Scraper District",
       -- t.top_500 as "Top 500 Districts",
       -- t.top_200 as "Top 200 Districts",
       t.pod as "Pod",
       (select distinct d.nces_id from edx.district_domains d where d.district_id = t.district_id) as NCES_ID,
       t.edex_members_by_month_cum as "EdEx_Members (cumulative)", -- "EdEx Members",
       t.overall_visits as "EdEx Overall Visitors",
       t.visits_by_month as "EdEx Visitors by Month",
       ifnull(m.mau, 0) as "MAU",
       ifnull(m.rmau_1, 0) as "RMAU",
       ifnull(m.rmau_2, 0) as "rMAU",
       ifnull(m.resurrected_mau, 0) as "Resurrected MAU",
       t.edex_members_by_month as "New EdEx Members by Month", -- "EdEx Members by Month",
       t.cum_acquisition_by_members as "Acquisition by EdEx Members (cumulative)", -- "Acquisition by EdEx Members",
       t.rate_content_acquisition_by_members as "Rate of Content Acquisition by Members", -- "Rate of Content Acquisition by Members",
       t.acquisition_by_month as "Unique educators who acquired content (by Month)", -- "Acquisition by Month",
       t.rate_content_acquisition_by_month as "Rate of Content Acquisition by Month",
       t.first_time_acquisition_by_month as "Unique educators - First Time Acquisition by Month", -- "First Time Acquisition by Month",
       t.rate_first_content_acquisition_by_members as "Rate of First Content Acquisition by Members", -- "Rate of First Content Acquisition by Members",
       t.repeat_acquisition_by_month as "Unique members - Repeat Acquisition by Month", -- "Repeat Acquisition by Month",
       t.rate_repeat_content_acquisition_by_members as "Rate of Repeat Content Acquisition by Members",
       t.cum_express_correlation_logins as "Express Correlation Logins (cumulative)", -- "Express Correlation Logins",
       t.rate_correlated_express_logins as "Rate of Correlated Express Logins",
       t.express_correlation_logins as "Unique Members-Express Correlation Logins by Month", -- "Express Correlation Logins by Month",
       t.rate_correlated_express_logins_monthly as "Rate of Correlated Express Monthly Logins",
       t.cum_express_causation_logins as "Express Causation Logins (cumulative)", -- "Express Causation Logins",
       t.rate_causation_express_logins as "Rate of Causation Express Logins",
       t.express_causation_logins as "Unique Members-Express Causation Logins by Month", -- "Express Causation Logins by Month",
       t.rate_causation_express_logins_monthly as "Rate of Causation Express Monthly Logins",
       t.cum_cc_correlation_logins as "CC Correlation Logins (cumulative)", -- "CC Correlation Logins",
       t.rate_cc_correlation_logins as "Rate of Correlated CC Logins",
       t.cc_correlation_logins as "Unique Members-CC Correlation Logins by Month", -- "CC Correlation Logins by Month",
       t.rate_cc_correlation_logins_monthly as "Rate of Correlated CC Monthly Logins",
       t.cum_cc_causation_logins as "CC Causation Logins (cumulative)", -- "CC Causation Logins",
       t.rate_cc_causation_logins as "Rate of Causation CC Logins",
       t.cc_causation_logins as "CC Causation Logins by Month",
       t.rate_cc_causation_logins_monthly as "Rate of Causation CC Monthly Logins"
from edx.rpt_district_edex_activity_monthly_fin t
left join edx.rpt_district_importing_mau m on m.district_id = t.district_id
                                       and m.fiscal_yr_and_per = t.fiscal_yr_and_per
-- join edx.district_domains d on d.district_id = t.district_id
-- join edx.district_domains d on d.district_id = t.district_id
-- join edx.hana_dim_date dd on dd.fiscal_yr_and_per = t.fiscal_yr_and_per
where t.fiscal_yr_and_per > 202112
and t.isCurrent = 1
and exists (select 1 from edx.district_domains d where d.district_id = t.district_id and d.domain_class = 'K12')
order by t.fiscal_yr_and_per desc, t.school_district_name;
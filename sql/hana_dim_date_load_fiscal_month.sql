select dd.fiscal_yr_and_qtr_desc,
       dd.fiscal_yr_and_per,
       dd.fiscal_yr_and_per_desc,       
       min(dd.calendar_date) as first_day, 
       max(dd.calendar_date) as last_day,
       case when (select distinct t.fiscal_yr_and_per from edx.hana_dim_date t where t.calendar_date = current_date()) = dd.fiscal_yr_and_per then '1'
            else '0'
	   end as current_fiscal_yr_and_per
from edx.hana_dim_date dd
where dd.fiscal_yr in (2025, 2024, 2023, 2022, 2021, 2020)
-- and dd.fiscal_yr_and_per <= (select t.fiscal_yr_and_per from edx.hana_dim_date t where t.calendar_date = current_date())
group by dd.fiscal_yr_and_per,
         dd.fiscal_yr_and_per_desc,
         dd.fiscal_yr_and_qtr_desc
order by dd.fiscal_yr_and_per desc;


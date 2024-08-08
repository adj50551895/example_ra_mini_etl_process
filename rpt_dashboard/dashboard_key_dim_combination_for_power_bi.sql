select distinct
       t.fiscal_yr_and_qtr_desc,
       t.fiscal_yr_and_per,
       t.fiscal_yr_and_per_desc,
       t.class,
       t.countryCode,
       concat(t.countryCode, '-', t.class) as countryCode_class,
       concat(t.fiscal_yr_and_per, '-', t.countryCode, '-', t.class) as fiscal_yr_and_per_class_countryCode
from edx.dashboard_final_results t;
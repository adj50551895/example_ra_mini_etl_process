SELECT
  dd.fiscal_yr_and_qtr_desc AS fiscal_yr_and_qtr,
  dd.fiscal_yr_and_per_desc AS fiscal_yr_and_per,
  t.country,
  t.class,
  COUNT(*) AS visitors -- ,
  -- SUM(COUNT(*)) OVER (PARTITION BY dd.fiscal_yr_and_per_desc, dd.fiscal_yr_and_per, t.country) AS total_by_month_country
FROM
  edx.fullstory_visitors_page_details_fin t
JOIN
  ra.hana_dim_date_temp dd ON dd.fiscal_yr_and_per = t.fiscal_yr_and_per
GROUP BY
  dd.fiscal_yr_and_qtr_desc,
  dd.fiscal_yr_and_per_desc,
  t.country,
  t.class
ORDER BY
  dd.fiscal_yr_and_qtr_desc,
  dd.fiscal_yr_and_per_desc,
  t.country,
  t.class;
  
  -- ---
  /*
  SELECT
  dd.fiscal_yr_and_qtr_desc AS fiscal_yr_and_qtr,
  dd.fiscal_yr_and_per_desc AS fiscal_yr_and_per,
  t.country,
  COUNT(*) AS visitors -- ,
  -- SUM(COUNT(*)) OVER (PARTITION BY dd.fiscal_yr_and_per_desc, dd.fiscal_yr_and_per, t.country) AS total_by_month_country
FROM
  edx.fullstory_visitors_page_details_fin t
JOIN
  ra.hana_dim_date_temp dd ON dd.fiscal_yr_and_per = t.fiscal_yr_and_per
GROUP BY
  dd.fiscal_yr_and_qtr_desc,
  dd.fiscal_yr_and_per_desc,
  t.country
ORDER BY
  dd.fiscal_yr_and_qtr_desc,
  dd.fiscal_yr_and_per_desc,
  t.country;
  */
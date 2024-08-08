-- May contain fiscal months gap (@current_fiscal_yr_and_per)
-- If the date of the last start of the report and the current date of the start of the report are in a different fiscal month, it is necessary to start the report twice - for the last two fiscal months.

-- link between edex and expres visitors
-- run after importing mcietl_web_visits_detailed_edex_clicks and mcietl_web_visits_detailed_express_clicks tables

-- current month
select dd.fiscal_yr_and_per
into @current_fiscal_yr_and_per
from edx.hana_dim_date dd 
where dd.calendar_date = current_date();

-- prev month
select dd.fiscal_yr_and_per
into @report_fiscal_yr_and_per
from edx.hana_dim_date dd 
where dd.calendar_date = (select max(edx_date) from hdp.stg_edex_express_dates);

-- if current month is different than the prev month
select if(@current_fiscal_yr_and_per != @report_fiscal_yr_and_per, 1, 0)
into @prev_month;

-- set @current_fiscal_yr_and_per = 202305;
-- set @prev_month = 0;

-- prev_month, report_fiscal_yr_and_per
delete from hdp.stg_edex_express_dates where edx_fiscal_yr_and_per = @report_fiscal_yr_and_per and 1=@prev_month;

insert into hdp.stg_edex_express_dates
select distinct edx.fiscal_yr_and_per as edx_fiscal_yr_and_per,
                exp.fiscal_yr_and_per as exp_fiscal_yr_and_per,
                exp.guid,
                -- exp.visid_high,
                -- exp.visid_low,
                -- exp.visit_num,
                -- exp.first_expresss_time as first_expresss_time,
                date(edx.date_time) as edx_date
from hdp.mcietl_web_visits_detailed_edex_clicks edx
join hdp.mcietl_web_visits_detailed_express_clicks exp on exp.visid_high = edx.visid_high
                                                      and exp.visid_low = edx.visid_low
                                                      and exp.visit_num = edx.visit_num
where edx.date_time <= exp.last_expresss_time
and edx.fiscal_yr_and_per = @report_fiscal_yr_and_per
and 1=@prev_month;


-- current_month
delete from hdp.stg_edex_express_dates where edx_fiscal_yr_and_per = @current_fiscal_yr_and_per;

insert into hdp.stg_edex_express_dates
select distinct edx.fiscal_yr_and_per as edx_fiscal_yr_and_per,
                exp.fiscal_yr_and_per as exp_fiscal_yr_and_per,
                exp.guid,
                -- exp.visid_high,
                -- exp.visid_low,
                -- exp.visit_num,
                -- exp.first_expresss_time as first_expresss_time,
                date(edx.date_time) as edx_date
from hdp.mcietl_web_visits_detailed_edex_clicks edx
join hdp.mcietl_web_visits_detailed_express_clicks exp on exp.visid_high = edx.visid_high
                                                      and exp.visid_low = edx.visid_low
                                                      and exp.visit_num = edx.visit_num
where edx.date_time <= exp.last_expresss_time
and edx.fiscal_yr_and_per = @current_fiscal_yr_and_per;
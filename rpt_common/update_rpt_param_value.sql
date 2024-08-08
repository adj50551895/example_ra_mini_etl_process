set @param_value = '202312';

-- Fullstory visitors
set @fullstory_param_value = @param_value;
update edx.rpt_params set param_value = @fullstory_param_value, 
                          dateUpdated = current_timestamp() 
where report_name = 'rpt_fullstory_visitors' 
and param_name = 'rpt_fiscal_month';

-- Dashboard fy
set @dashboard_param_value = @param_value;
update edx.rpt_params set param_value = @dashboard_param_value, 
                          dateUpdated = current_timestamp() 
where report_name = 'rpt_dashboard' 
and param_name = 'rpt_fiscal_month';
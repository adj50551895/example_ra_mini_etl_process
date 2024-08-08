-- to update parameter values use script from the file: XXXX\rpt_common\update_rpt_param_value.sql

select *
from edx.rpt_params p
where p.type = 'manually';

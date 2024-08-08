insert into ra.fullstory_tmp_visitors_page_details_fiscal_month
select t.*,
       e.fiscal_yr_and_per,
       current_date() as date_inserted
from ra.fullstory_tmp_visitors_page_details t
left join edx.fullstory_visitors_hana_dim_date e on t.EventStart between e.first_day and e.last_day;
/* where not exists (
    select 1
    from ra.fullstory_tmp_visitors_page_details_fiscal_month t1
    where t1.IndvId = t.IndvId
    and   t1.UserId = t.UserId
    and   t1.PageId = t.PageId
    and   t1.fiscal_yr_and_per = e.fiscal_yr_and_per
    and   t1.segmentId = t.segmentId);
*/
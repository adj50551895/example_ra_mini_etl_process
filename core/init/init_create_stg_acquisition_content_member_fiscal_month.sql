create table els.stg_acquisition_content_member_fiscal_month as
select coalesce(e.memberId, e.sessionId) as memberId,
       dd.fiscal_yr_and_per_desc,
       dd.fiscal_yr_and_per,
       count(*) as cnt_memberId
from els.events e
join edx.hana_dim_date dd on dd.calendar_date = e.event_date
join els.stg_acquisition_content_events acq on acq.event = e.event
group by e.memberId,
         dd.fiscal_yr_and_per_desc,
         dd.fiscal_yr_and_per;
-- member first time content acquisition by fiscal_yr_and_per
-- run after importing els.events

-- create table els.stg_member_first_time_content_acquisition as
insert into els.stg_member_first_time_content_acquisition
select dd.fiscal_yr_and_per,
       e.memberId,
       min(e.event_date) as min_event_date
from els.events e -- tabela event nema sve godine
join edx.hana_dim_date dd on dd.calendar_date = e.event_date
join els.stg_acquisition_content_events acqe on acqe.event = e.event
where e.memberId is not null
and not exists (select 1 from els.stg_member_first_time_content_acquisition t where t.fiscal_yr_and_per = dd.fiscal_yr_and_per and t.memberId = e.memberId)
group by dd.fiscal_yr_and_per,
         e.memberId;
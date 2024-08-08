-- May contain fiscal months gap (@current_fiscal_yr_and_per)
-- If the date of the last start of the report and the current date of the start of the report are in a different fiscal month, it is necessary to start the report twice - for the last two fiscal months.

-- members that acquired content
-- run after importing els.events

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
-- set @cur_month = 0;


-- prev month
delete from els.stg_events_acquisition_content where fiscal_yr_and_per = @report_fiscal_yr_and_per and 1=@prev_month;

insert into els.stg_events_acquisition_content
select dd.fiscal_yr_and_per,
       e.id,
       e.event_date,
       e.memberId,
       e.event,
       e.eventLevel,
       case when e.entityType is null and r.id is not null then 'resource'
            when e.entityType is null and c.id is not null then 'course'
            when e.entityType is null and s.id is not null then 'staticpage'
            else e.entityType
       end as entityType,
       e.entityId,
       e.sessionId,
       case when (r.id is not null or c.id is not null or s.id is not null) then a.type
            else null
       end link_type
from els.events e
join els.stg_acquisition_content_events a on a.event = e.event
join edx.hana_dim_date dd on dd.calendar_date = e.event_date
left join edx.Resource r on r.id = e.entityId
left join edx.Course c on c.id = e.entityId
left join edx.StaticPage s on s.id COLLATE utf8mb4_general_ci = e.entityId
where dd.fiscal_yr_and_per = @report_fiscal_yr_and_per
and 1=@prev_month;

-- Adding events (v1.enrollment.enrolled) from Enrollment
insert into els.stg_events_acquisition_content
select distinct 
       dd.fiscal_yr_and_per,
       left(concat(enr.memberId, enr.courseId), 50) as id, -- id concatenated
       date(enr.createdAt) as event_date,
       enr.memberId,
       'v1.enrollment.enrolled' as event,
       'engage' as eventLevel,
       'course' as entityType,
       enr.courseId as entityId,
       enr.memberID as sessionId,
       'course' as link_type
from edx.Enrollment enr
join edx.hana_dim_date dd on dd.calendar_date = date(enr.createdAt)
where dd.fiscal_yr_and_per = @report_fiscal_yr_and_per
and 1=@prev_month;


-- current month
delete from els.stg_events_acquisition_content where fiscal_yr_and_per = @current_fiscal_yr_and_per;

insert into els.stg_events_acquisition_content
select dd.fiscal_yr_and_per,
       e.id,
       e.event_date,
       e.memberId,
       e.event,
       e.eventLevel,
       case when e.entityType is null and r.id is not null then 'resource'
            when e.entityType is null and c.id is not null then 'course'
            when e.entityType is null and s.id is not null then 'staticpage'
            else e.entityType
       end as entityType,
       e.entityId,
       e.sessionId,
       case when (r.id is not null or c.id is not null or s.id is not null) then a.type
            else null
       end link_type
from els.events e
join els.stg_acquisition_content_events a on a.event = e.event
join edx.hana_dim_date dd on dd.calendar_date = e.event_date
left join edx.Resource r on r.id = e.entityId
left join edx.Course c on c.id = e.entityId
left join edx.StaticPage s on s.id COLLATE utf8mb4_general_ci = e.entityId
where dd.fiscal_yr_and_per = @current_fiscal_yr_and_per;

-- Adding events (v1.enrollment.enrolled) from Enrollment
insert into els.stg_events_acquisition_content
select distinct 
       dd.fiscal_yr_and_per,
       left(concat(enr.memberId, enr.courseId), 50) as id, -- id concatenated
       date(enr.createdAt) as event_date,
       enr.memberId,
       'v1.enrollment.enrolled' as event,
       'engage' as eventLevel,
       'course' as entityType,
       enr.courseId as entityId,
       enr.memberID as sessionId,
       'course' as link_type
from edx.Enrollment enr
join edx.hana_dim_date dd on dd.calendar_date = date(enr.createdAt)
where dd.fiscal_yr_and_per = @current_fiscal_yr_and_per;
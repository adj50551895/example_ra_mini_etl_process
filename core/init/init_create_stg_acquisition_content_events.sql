create table edx.content_acquisition_events as
select 'v1.file.fetched' as event union
select 'resource.click.downloadToDevice' union
select 'resource.click.sendToGD' union
select 'resource.click.sendToOD' union
select 'v1.resource.export-to-gd-trigger' union
select 'v1.resource.export-to-gd-success' union
select 'v1.resource.export-zip-trigger' union
select 'v1.resource.export-zip-success' union
select 'v1.resource.export-to-od-trigger' union
select 'v1.resource.export-to-od-success' union
select 'v1.engaged.resourceLink' union
select 'resource.get' union
select 'resource.click.inlineLink' union
select 'resource.click.weblink' union
select 'resource.click.shareDialog' union
select 'resource.click.share' union
select 'resource.click.ccxTemplateLink' union
select 'resource.click.ccxTemplateLink.inline' union
select 'resource.click.ccxAcquisition' union
select 'course.click.ccxTemplateLink' union
select 'course.click.ccxTemplateLink.inline' union
select 'v1.enrollment.created' union
select 'v1.enrollment.enrolled' union
select 'course.enroll' union
select 'v1.enrollment.course-started' union
select 'resouce.click.acrobatBuy' union
select 'resouce.click.ccxBuy' union
select 'resource.click.acrobatAcquisition' union
select 'resource.click.getFreeTrial' union
select 'resource.video.play' union
select 'staticpage.click.ccx.Acquisition' union
select 'staticpage.click.ccxTemplateLink' union
select 'staticpage.click.instructions';

create table els.stg_acquisition_content_events as
select   e.event,      
         case when e.event in (
                'v1.file.fetched'
                ,'resource.click.downloadToDevice'
                ,'resource.click.sendToGD'
                ,'resource.click.sendToOD'
                ,'v1.resource.export-to-gd-trigger'
                ,'v1.resource.export-to-gd-success'
                ,'v1.resource.export-zip-trigger'
                ,'v1.resource.export-zip-success'
                ,'v1.resource.export-to-od-trigger'
                ,'v1.resource.export-to-od-success'
                ,'resource.get'
                ,'v1.engaged.resourceLink'
                ,'resource.click.inlineLink'
                ,'resource.click.weblink'
                ,'resource.click.shareDialog'
                ,'resource.click.share'
                ,'resource.video.play'
                ,'resource.click.getFreeTrial'
                ,'resource.click.ccxTemplateLink'
                ,'resource.click.ccxTemplateLink.inline'
                ,'resource.click.ccxAcquisition'
                ,'resouce.click.acrobatBuy'
                ,'resouce.click.ccxBuy'
                ,'resource.click.acrobatAcquisition'
                ,'staticpage.click.ccx.Acquisition'
                ,'staticpage.click.ccxTemplateLink'
                ,'staticpage.click.instructions') then 'resource'
            when e.event in (
                'course.click.ccxTemplateLink'
                ,'course.click.ccxTemplateLink.inline'
                ,'v1.enrollment.created'
                ,'v1.enrollment.enrolled'
                ,'course.enroll'
                ,'v1.enrollment.course-started') then 'course'
            else null
        end as type
from edx.content_acquisition_events e;


-- 54,571,608
-- 54,571,608
-- 2,038,909
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
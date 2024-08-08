set @rpt_start = '2020-11-27'; -- stavi koji je pocetak FY 21


-- resource_rating
drop temporary table if exists ra.tmp_content_tr_last_rating;
create temporary table ra.tmp_content_tr_last_rating as 
select t.entityId,
       t.createdBy,       
       max(t.rating) as rating,
       max(t.createdAt) as createdAt
from edx.rating t
where t.rating >= 0
and t.entityType = 'Resource'
and t.createdAt >= @rpt_start
group by t.entityId,
         t.createdBy;

drop temporary table if exists ra.tmp_content_tr_resource_rating;
create temporary table ra.tmp_content_tr_resource_rating as 
select r.entityId,
       avg(r.rating) as rating,
       count(distinct r.createdBy) as rating_num
from ra.tmp_content_tr_last_rating r
group by r.entityId;

-- resource_geo
drop temporary table if exists ra.tmp_content_tr_resource_geo;
create temporary table ra.tmp_content_tr_resource_geo as 
select r.id as resourceId,
       ifnull(cc.country_code, 'Unknown') as country_code,
       ifnull(cc.country, 'Unknown') as country,
       ifnull(cc.region, 'Unknown') as region,
       case when r.createdBy != '82303c61-6102-11e2-9a53-12313b016471' then 'User Generated'
            else 'Adobe for Education'
	   end as creator_type
from edx.Resource r
join edx.Member m on m.id = r.createdBy
left join edx.region_country_codes cc on cc.country_code = m.countryCode;

-- resource to product
drop temporary table if exists ra.tmp_content_tr_resourceToProductPrimary;
create temporary table ra.tmp_content_tr_resourceToProductPrimary as 
select t.resourceId,
       GROUP_CONCAT(DISTINCT CONCAT(p.title)) as products
from edx.resourceToProduct t
join edx.Product p on p.id = t.products
where t.layer = 'primary'
group by t.resourceId;

drop temporary table if exists ra.tmp_content_tr_resourceToProductSecondary;
create temporary table ra.tmp_content_tr_resourceToProductSecondary as 
select t.resourceId,
       GROUP_CONCAT(DISTINCT CONCAT(p.title)) as products
from edx.resourceToProduct t
join edx.Product p on p.id = t.products
where t.layer = 'secondary'
group by t.resourceId;

-- resource to subject
drop temporary table if exists ra.tmp_content_tr_resourceToSubjectPrimary;
create temporary table ra.tmp_content_tr_resourceToSubjectPrimary as
select t.resourceId,
       REPLACE(GROUP_CONCAT(DISTINCT CONCAT(s.i18nLabel)), 'i18n.subjects.', '') as subjects
from edx.resourceToSubject t
join edx.Subject s on s.id = t.subjects
where t.layer = 'primary'
group by t.resourceId;

drop temporary table if exists ra.tmp_content_tr_resourceToSubjectSecondary;
create temporary table ra.tmp_content_tr_resourceToSubjectSecondary as
select t.resourceId,
       REPLACE(GROUP_CONCAT(DISTINCT CONCAT(s.i18nLabel)), 'i18n.subjects.', '') as subjects
from edx.resourceToSubject t
join edx.Subject s on s.id = t.subjects
where t.layer = 'secondary'
group by t.resourceId;

-- resource to academic level, age
drop temporary table if exists ra.tmp_content_tr_resourceToAcademicLevelPrimary;
create temporary table ra.tmp_content_tr_resourceToAcademicLevelPrimary as
select t.resourceId,
       GROUP_CONCAT(DISTINCT CONCAT(a.urlLabel)) as academicLevels
from edx.resourceToAcademicLevel t
join edx.AcademicLevel a on a.id = t.academicLevels
where t.layer = 'primary'
group by t.resourceId;

drop temporary table if exists ra.tmp_content_tr_resourceToAcademicLevelSecondary;
create temporary table ra.tmp_content_tr_resourceToAcademicLevelSecondary as
select t.resourceId,
       GROUP_CONCAT(DISTINCT CONCAT(a.urlLabel)) as academicLevels
from edx.resourceToAcademicLevel t
join edx.AcademicLevel a on a.id = t.academicLevels
where t.layer = 'secondary'
group by t.resourceId;

-- Base resource table
drop table if exists ra.tmp_content_tr_base;
create table ra.tmp_content_tr_base as
select r.id,
       r.vanityURL,
       r.publishedAt,
       r.status,
       r.title,
       case when r.siteID = 'a5ac163e-67f5-4464-aad1-faac2d05dbea' then 'EN'
            when r.siteID = '6e926cec-477b-11e6-b906-1712142eaacb' then 'JP'
            when r.siteID = '7ce2909c-477b-11e6-b906-1712142eaacb' then 'DE'
            else 'N/A'
       end as language,
       rr.rating,
       rr.rating_num,
       gc.country_code,
       gc.country,
       gc.region,
       gc.creator_type,
       rpp.products as productsPrimary,
       rps.products as productsSecondary,
       rsp.subjects as subjectsPrimary,
       rss.subjects as subjectsSecondary,
       rap.academicLevels as academicLevelsPrimary,
       ras.academicLevels as academicLevelsSecondary
from edx.Resource r
left join ra.tmp_content_tr_resource_rating rr on rr.entityId = r.id
left join ra.tmp_content_tr_resource_geo gc on gc.resourceId = r.id
left join ra.tmp_content_tr_resourceToProductPrimary rpp on rpp.resourceId = r.id
left join ra.tmp_content_tr_resourceToProductSecondary rps on rps.resourceId = r.id
left join ra.tmp_content_tr_resourceToSubjectPrimary rsp on rsp.resourceId = r.id
left join ra.tmp_content_tr_resourceToSubjectSecondary rss on rss.resourceId = r.id
left join ra.tmp_content_tr_resourceToAcademicLevelPrimary rap on rap.resourceId = r.id
left join ra.tmp_content_tr_resourceToAcademicLevelSecondary ras on ras.resourceId = r.id
where r.status = 'active'
and r.public = '1';


-- ---------------------------------------------------------------------------------------------------
-- Dynamic Data -- Visits Downloads
-- events
drop table if exists ra.tmp_content_detail_tr_events;
create table ra.tmp_content_detail_tr_events as
select e.event_date,
       e.entityID,
       e.event,
       e.entityType,
       e.eventLevel,
       e.memberId,
       e.sessionId,
       case when e.event_date >= DATE_SUB(current_date(),INTERVAL 30 DAY) then 1
            else 0
	   end as days_30,
       case when e.event_date >= DATE_SUB(current_date(),INTERVAL 90 DAY) then 1
            else 0
	   end as days_90,
       case when e.event_date >= DATE_SUB(current_date(),INTERVAL 24 MONTH) then 1
            else 0
	   end as months_24,
       case when acq.event is not null then 1
            else 0
       end as isContentAcquisition
from els.events e
left join els.stg_acquisition_content_events acq on acq.event = e.event
where e.entityType in ('resource', 'staticpage')
and e.event_date > @rpt_start;

-- events_visits_downloads_30_days
drop temporary table if exists ra.tmp_content_detail_tr_visits_downloads_30;
create temporary table ra.tmp_content_detail_tr_visits_downloads_30 as
select t.entityID resourceID,
       sum(case when t.eventLevel = 'view' then 1 end) total_views_30_days,
       sum(case when t.eventLevel = 'view' and t.memberID is null then 1 end) guest_views_30_days,
       sum(case when t.eventLevel = 'view' and t.memberID is not null then 1 end) member_views_30_days,
       count(distinct case when t.eventLevel = 'view' then concat(ifnull(t.memberID, t.sessionID),t.event_date) end) unique_daily_views_30_days,
       count(distinct case when t.eventLevel = 'view' then ifnull(t.memberID, t.sessionID) end) unique_views_30_days,
       count(distinct case when t.eventLevel = 'download' then t.memberID end) unique_downloads_30_days
from ra.tmp_content_detail_tr_events t
where 1=1 -- entityType = 'resource' 
and t.eventLevel in ('download','view')
and t.event not in ('resource.click.ccxTemplateLink.inline', 'resource.click.ccxTemplateLink', 'resource.click.ccxAcquisition', 'staticpage.click.ccx.Acquisition', 'staticpage.click.ccxTemplateLink', 'staticpage.click.instructions')
and t.days_30 = 1
group by t.entityID;

-- events_visits_downloads_90_days
drop temporary table if exists ra.tmp_content_detail_tr_visits_downloads_90;
create temporary table ra.tmp_content_detail_tr_visits_downloads_90 as
select t.entityID resourceID,
       sum(case when t.eventLevel = 'view' then 1 end) total_views_90_days,
       sum(case when t.eventLevel = 'view' and t.memberID is null then 1 end) guest_views_90_days,
       sum(case when t.eventLevel = 'view' and t.memberID is not null then 1 end) member_views_90_days,
       count(distinct case when t.eventLevel = 'view' then concat(ifnull(t.memberID, t.sessionID), t.event_date) end) unique_daily_views_90_days,
       count(distinct case when t.eventLevel = 'view' then ifnull(t.memberID, t.sessionID) end) unique_views_90_days,
       count(distinct case when t.eventLevel = 'download' then t.memberID end) unique_downloads_90_days
from ra.tmp_content_detail_tr_events t
where 1=1 -- entityType = 'resource' 
and t.eventLevel in ('download','view')
and t.event not in ('resource.click.ccxTemplateLink.inline', 'resource.click.ccxTemplateLink', 'resource.click.ccxAcquisition', 'staticpage.click.ccx.Acquisition', 'staticpage.click.ccxTemplateLink', 'staticpage.click.instructions')
and t.days_90 = 1
group by t.entityID;

-- events_visits_downloads_24_months
drop temporary table if exists ra.tmp_content_detail_tr_visits_downloads_24;
create temporary table ra.tmp_content_detail_tr_visits_downloads_24 as
select t.entityID resourceID,
       sum(case when t.eventLevel = 'view' then 1 end) total_views_24_months,
       sum(case when t.eventLevel = 'view' and t.memberID is null then 1 end) guest_views_24_months,
       sum(case when t.eventLevel = 'view' and t.memberID is not null then 1 end) member_views_24_months,
       count(distinct case when t.eventLevel = 'view' then concat(ifnull(t.memberID, t.sessionID), t.event_date) end) unique_daily_views_24_months,
       count(distinct case when t.eventLevel = 'view' then ifnull(t.memberID, t.sessionID) end) unique_views_24_months,
       count(distinct case when t.eventLevel = 'download' then t.memberID end) unique_downloads_24_months
from ra.tmp_content_detail_tr_events t
where 1=1 -- entityType = 'resource' 
and t.eventLevel in ('download', 'view')
and t.event not in ('resource.click.ccxTemplateLink.inline', 'resource.click.ccxTemplateLink', 'resource.click.ccxAcquisition', 'staticpage.click.ccx.Acquisition', 'staticpage.click.ccxTemplateLink', 'staticpage.click.instructions')
and t.months_24 = 1
group by t.entityID;


-- ---------------------------------------------------------------------------------------------------
-- Total Acquisition/Click through/Shares /Attachment previews

-- events_click_through_30_days
drop temporary table if exists ra.tmp_content_detail_tr_click_through_30;
create temporary table ra.tmp_content_detail_tr_click_through_30 as
select t.entityID resourceID,
       sum(case when (t.event='resource.click.shareDialog' or t.event='resource.click.share' and t.memberID is null) then 1 end) guest_shares_30_days,
       sum(case when (t.event='resource.subview.asset' or t.event='resource.seeall' and t.memberID is null) then 1 end) guest_previews_30_days,
       sum(case when (t.event='resource.click.ccxTemplateLink.inline'or t.event= 'resource.click.ccxTemplateLink' or t.event='resource.click.ccxAcquisition' or t.event='staticpage.click.ccx.Acquisition' or t.event='staticpage.click.ccxTemplateLink' or t.event='staticpage.click.instructions' and t.memberID is null) then 1 end) guest_clickthroughs_30_days,
       count(distinct case when (t.event='resource.click.shareDialog' or t.event='resource.click.share') then t.memberID end) unique_member_shares_30_days,
       count(distinct case when (t.event='resource.subview.asset' or t.event='resource.seeall') then t.memberID end) unique_member_preview_30_days,
       count(distinct case when (t.event='resource.click.ccxTemplateLink.inline' or t.event= 'resource.click.ccxTemplateLink' or t.event='resource.click.ccxAcquisition' or t.event='staticpage.click.ccx.Acquisition' or t.event='staticpage.click.ccxTemplateLink' or t.event='staticpage.click.instructions') then t.memberID end) unique_member_clickthrough_30_days,
       count(distinct case when (t.isContentAcquisition = 1 or t.eventLevel = 'download') then t.memberID end) unique_content_acquisition_30_days
from ra.tmp_content_detail_tr_events t
where 1=1 -- t.entityType = 'resource' 
and t.days_30 = 1
group by t.entityID;

-- events_click_through_90_days
drop temporary table if exists ra.tmp_content_detail_tr_click_through_90;
create temporary table ra.tmp_content_detail_tr_click_through_90 as
select t.entityID resourceID,
       sum(case when (t.event='resource.click.shareDialog' or t.event='resource.click.share' and t.memberID is null) then 1 end) guest_shares_90_days,
       sum(case when (t.event='resource.subview.asset' or t.event='resource.seeall' and t.memberID is null) then 1 end) guest_previews_90_days,
       sum(case when (t.event='resource.click.ccxTemplateLink.inline'or t.event= 'resource.click.ccxTemplateLink' or t.event='resource.click.ccxAcquisition' or t.event='staticpage.click.ccx.Acquisition' or t.event='staticpage.click.ccxTemplateLink' or t.event='staticpage.click.instructions' and t.memberID is null) then 1 end) guest_clickthroughs_90_days,
       count(distinct case when (t.event='resource.click.shareDialog' or t.event='resource.click.share') then t.memberID end) unique_member_shares_90_days,
       count(distinct case when (t.event='resource.subview.asset' or t.event='resource.seeall') then t.memberID end) unique_member_preview_90_days,
       count(distinct case when (t.event='resource.click.ccxTemplateLink.inline' or t.event= 'resource.click.ccxTemplateLink' or t.event='resource.click.ccxAcquisition' or t.event='staticpage.click.ccx.Acquisition' or t.event='staticpage.click.ccxTemplateLink' or t.event='staticpage.click.instructions') then t.memberID end) unique_member_clickthrough_90_days,
       count(distinct case when (t.isContentAcquisition = 1 or t.eventLevel = 'download') then t.memberID end) unique_content_acquisition_90_days
from ra.tmp_content_detail_tr_events t
where 1=1 -- t.entityType = 'resource' 
and t.days_90 = 1
group by t.entityID;

-- events_click_through_24_months
drop temporary table if exists ra.tmp_content_detail_tr_click_through_24;
create temporary table ra.tmp_content_detail_tr_click_through_24 as
select t.entityID resourceID,
       sum(case when (t.event='resource.click.shareDialog' or t.event='resource.click.share' and t.memberID is null) then 1 end) guest_shares_24_months,
       sum(case when (t.event='resource.subview.asset' or t.event='resource.seeall' and t.memberID is null) then 1 end) guest_previews_24_months,
       sum(case when (t.event='resource.click.ccxTemplateLink.inline'or t.event= 'resource.click.ccxTemplateLink' or t.event='resource.click.ccxAcquisition' or t.event='staticpage.click.ccx.Acquisition' or t.event='staticpage.click.ccxTemplateLink' or t.event='staticpage.click.instructions' and t.memberID is null) then 1 end) guest_clickthroughs_24_months,
       count(distinct case when (t.event='resource.click.shareDialog' or t.event='resource.click.share') then t.memberID end) unique_member_shares_24_months,
       count(distinct case when (t.event='resource.subview.asset' or t.event='resource.seeall') then t.memberID end) unique_member_preview_24_months,
       count(distinct case when (t.event='resource.click.ccxTemplateLink.inline' or t.event= 'resource.click.ccxTemplateLink' or t.event='resource.click.ccxAcquisition' or t.event='staticpage.click.ccx.Acquisition' or t.event='staticpage.click.ccxTemplateLink' or t.event='staticpage.click.instructions') then t.memberID end) unique_member_clickthrough_24_months,
       count(distinct case when (t.isContentAcquisition = 1 or t.eventLevel = 'download') then t.memberID end) unique_content_acquisition_24_months
from ra.tmp_content_detail_tr_events t
where 1=1 -- t.entityType = 'resource' 
and t.months_24 = 1
group by t.entityID;


-- ---------------------------------------------------------------------------------------------------
-- Resource Favorites
drop temporary table if exists ra.tmp_content_detail_tr_favorite_base;
create temporary table ra.tmp_content_detail_tr_favorite_base as
select f.entityID, --  as resourceId,
       f.entityType,
       max(f.createdAt) as createdAt,
       f.createdBy,
       f.status,
       datediff(current_date(), max(f.createdAt)) as date_diff,
       case when (datediff(current_date(), max(f.createdAt)) <= 30) then 1
            else 0
	   end as fav_days_30,
       case when (datediff(current_date(), max(f.createdAt)) <= 60) then 1
            else 0
	   end as fav_days_60,
       case when (datediff(current_date(), max(f.createdAt)) <= 90) then 1
            else 0
	   end as fav_days_90,
       case when (datediff(current_date(), max(f.createdAt)) <= 730) then 1
            else 0
	   end as fav_days_730,
        case when (datediff(current_date(), max(f.createdAt)) <= 30 and f.status = 'active') then 1
            else 0
	   end as fav_days_30_active,
       case when (datediff(current_date(), max(f.createdAt)) <= 60 and f.status = 'active') then 1
            else 0
	   end as fav_days_60_active,
       case when (datediff(current_date(), max(f.createdAt)) <= 90 and f.status = 'active') then 1
            else 0
	   end as fav_days_90_active,
       case when (datediff(current_date(), max(f.createdAt)) <= 730 and f.status = 'active') then 1
            else 0
	   end as fav_days_730_active
from edx.Favorite f
join ra.tmp_content_tr_base b on b.id = f.entityID
-- join edx.Member m on m.id = f.createdBy
-- where f.entityId = '79bedf35-5e5c-4c86-a7b6-b51beaa5c03a'
group by f.entityID,
         f.entityType,
         f.createdBy,
         f.status;

drop temporary table if exists ra.tmp_content_detail_tr_resource_favorites;
create temporary table ra.tmp_content_detail_tr_resource_favorites as
select t.entityId as resourceID,
       sum(t.fav_days_30) as fav_days_30,
       sum(t.fav_days_60) as fav_days_60,
       sum(t.fav_days_90) as fav_days_90,
       sum(t.fav_days_730) as fav_days_730,
       sum(t.fav_days_30_active) as fav_days_30_active,
       sum(t.fav_days_60_active) as fav_days_60_active,
       sum(t.fav_days_90_active) as fav_days_90_active,
       sum(t.fav_days_730_active) as fav_days_730_active
from ra.tmp_content_detail_tr_favorite_base t
-- where t.entityId = '79bedf35-5e5c-4c86-a7b6-b51beaa5c03a'
group by t.entityId;


-- final table
update edx.rpt_content_detail_teaching_resources set isCurrent = '0' where isCurrent = '1';

-- drop table if exists edx.rpt_content_detail_teaching_resources;
-- create table edx.rpt_content_detail_teaching_resources as
insert into edx.rpt_content_detail_teaching_resources
select b.id as resourceId,
       b.title,
       b.rating,
       b.rating_num,
       b.country_code,
       b.country,
       b.vanityURL,
       b.publishedAt,
       b.status,
       b.language,
       b.region,
       b.creator_type,
       b.productsPrimary,
       b.productsSecondary,
       b.subjectsPrimary,
       b.subjectsSecondary,
       b.academicLevelsPrimary,
       b.academicLevelsSecondary,
       t1.total_views_30_days, -- "30 Days Total Content Views",
       t2.total_views_90_days, -- "90 Days Total Content Views",
       t3.total_views_24_months, -- "24 Months Total Content Views",
       t1.guest_views_30_days, -- "30 Days Guest Views",
       t2.guest_views_90_days, -- "90 Days Guest Views",
       t3.guest_views_24_months, -- "24 Months Guest Views",
       t1.member_views_30_days, -- "30 Days Member Views",
       t2.member_views_90_days, -- "90 Days Member Views",
       t3.member_views_24_months, -- "24 Months Member Views",
       t1.unique_daily_views_30_days, -- "30 Days Unique Daily Views",
       t2.unique_daily_views_90_days, -- "60 Days Unique Daily Views",
       t3.unique_daily_views_24_months, -- "24 Months Unique Daily Views",
       t1.unique_views_30_days, -- "30 Days Unique Member Views",
       t2.unique_views_90_days, -- "90 Days Unique Member Views",
       t3.unique_views_24_months, -- "24 Months Unique Member Views",
       t4.unique_content_acquisition_30_days,-- "30 Days Unique Content Acquisition"
       t5.unique_content_acquisition_90_days, -- "90 Days Unique Content Acquisition"
       t6.unique_content_acquisition_24_months, -- "24 Months Unique Content Acquisition"
       t1.unique_downloads_30_days, -- "30 Days Unique Downloads",
       t2.unique_downloads_90_days, -- "90 Days Unique Downloads",
       t3.unique_downloads_24_months, -- "24 Months Unique Downloads",
       t4.guest_previews_30_days, -- "30 Days Guest Previews",
       t5.guest_previews_90_days, -- "90 Days Guest Previews",
       t6.guest_previews_24_months, -- "24 Months Guest Previews",
       t4.unique_member_preview_30_days, -- "30 Days Member Previews",
       t5.unique_member_preview_90_days, -- "90 Days Member Previews",
       t6.unique_member_preview_24_months, -- "24 Months Member Previews",
       t4.guest_shares_30_days, -- "30 Days Guest Shares",
       t5.guest_shares_90_days, -- "90 Days Guest Shares",
       t6.guest_shares_24_months, -- "24 Months Guest Shares",
       t4.unique_member_shares_30_days, -- "30 Days Member Shares",
       t5.unique_member_shares_90_days, -- "90 Days Member Shares",
       t6.unique_member_shares_24_months, -- "24 Months Member Shares",
       --
       t4.guest_clickthroughs_30_days, -- "30 Days Guest Click-throughs",
       t5.guest_clickthroughs_90_days, -- "90 Days Guest Click-throughs",
       t6.guest_clickthroughs_24_months, -- "24 Months Guest Click-throughs",
       t4.unique_member_clickthrough_30_days, -- "30 Days Member Click-throughs",
       t5.unique_member_clickthrough_90_days, -- "90 Days Member Click-throughs",
       t6.unique_member_clickthrough_24_months, -- "24 Months Member Click-throughs",
       --
       t7.fav_days_30, -- "30 Days Number of Times Favorited",
       t7.fav_days_60, -- "60 Days Number of Times Favorited",
       t7.fav_days_90, -- "90 Days Number of Times Favorited",
       t7.fav_days_730, -- "24 Months of Times Favorited",
       t7.fav_days_30_active, -- "30 Days Members Currently Favorited",
       t7.fav_days_60_active, -- "60 Days Members Currently Favorited",
       t7.fav_days_90_active, -- "90 Days Members Currently Favorited",
       t7.fav_days_730_active, -- "24 Months Members Currently Favorited",
       current_timestamp() as createdAt,
       1 as isCurrent
from ra.tmp_content_tr_base b
left join ra.tmp_content_detail_tr_visits_downloads_30 t1 on t1.resourceID = b.id
left join ra.tmp_content_detail_tr_visits_downloads_90 t2 on t2.resourceID = b.id
left join ra.tmp_content_detail_tr_visits_downloads_24 t3 on t3.resourceID = b.id
left join ra.tmp_content_detail_tr_click_through_30 t4 on t4.resourceID = b.id
left join ra.tmp_content_detail_tr_click_through_90 t5 on t5.resourceID = b.id
left join ra.tmp_content_detail_tr_click_through_24 t6 on t6.resourceID = b.id
left join ra.tmp_content_detail_tr_resource_favorites t7 on t7.resourceID = b.id;
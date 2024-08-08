select distinct t.resourceId,
       t.title,
       t.rating,
       t.rating_num,
       t.country_code as "Country Code",
       t.country,
       t.vanityURL,
       t.publishedAt,
       IFNULL(tm.Priority, '  - ') as "Priority",
       t.status,
       t.language,
       t.region,
       t.creator_type,
       t.productsPrimary,
       t.productsSecondary,
       COALESCE(t.productsPrimary, t.productsSecondary) as Products,
       t.subjectsPrimary,
       t.subjectsSecondary,
       COALESCE(t.subjectsPrimary, t.subjectsSecondary) as Subjects,
       t.academicLevelsPrimary,
       t.academicLevelsSecondary,
       COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) as "Academic Level",
       case when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) like '%Higher_Education%' then 'HED'
		    when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) like '%All_Ages%' then 'HED'
            else 'K12'
	   end as "K12/HED",
       case when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) like '%Higher_Education%' then 'Higher Education'
            when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) like '%All_Ages%' then 'Higher Education'
            when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) like '%3rd-5th_Grade%' then 'Elementary School/Middle'
            when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) like '%6th-8th_Grade%' then 'Elementary School/Middle'
            when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) like '%Kindergarten%' then 'Elementary School/Middle'
            when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) like '%Early_Childhood%' then 'Elementary School/Middle'
            when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) like '%9th-12th_Grade%' then 'High School'
            when COALESCE(t.academicLevelsPrimary, t.academicLevelsSecondary) is null then null
            else 'ADD DEFINITION'            
	   end as "Elementary_Middle/High_School",
       IFNULL(t.total_views_30_days, 0) as "30 Days Total Content Views",
       IFNULL(t.total_views_90_days, 0) as "90 Days Total Content Views",
       IFNULL(t.total_views_24_months, 0) as "24 Months Total Content Views",
       IFNULL(t.guest_views_30_days, 0) as "30 Days Guest Views",
       IFNULL(t.guest_views_90_days, 0) as "90 Days Guest Views",
       IFNULL(t.guest_views_24_months, 0) as "24 Months Guest Views",
       IFNULL(t.member_views_30_days, 0) as "30 Days Member Views",
       IFNULL(t.member_views_90_days, 0) as "90 Days Member Views",
       IFNULL(t.member_views_24_months, 0) as "24 Months Member Views",
       IFNULL(t.unique_daily_views_30_days, 0) as "30 Days Unique Daily Views",
       IFNULL(t.unique_daily_views_90_days, 0) as "60 Days Unique Daily Views",
       IFNULL(t.unique_daily_views_24_months, 0) as "24 Months Unique Daily Views",
       IFNULL(t.unique_views_30_days, 0) as "30 Days Unique Member Views",
       IFNULL(t.unique_views_90_days, 0) as "90 Days Unique Member Views",
       IFNULL(t.unique_views_24_months, 0) as "24 Months Unique Member Views",
       IFNULL(t.unique_content_acquisition_30_days, 0) as "30 Days Unique Content Acquisition",
       IFNULL(t.unique_content_acquisition_90_days, 0) as "90 Days Unique Content Acquisition",
       IFNULL(t.unique_content_acquisition_24_months, 0) as "24 Months Unique Content Acquisition",
       IFNULL(t.unique_downloads_30_days, 0) as "30 Days Unique Downloads",
       IFNULL(t.unique_downloads_90_days, 0) as "90 Days Unique Downloads",
       IFNULL(t.unique_downloads_24_months, 0) as "24 Months Unique Downloads",
       IFNULL(t.guest_previews_30_days, 0) as "30 Days Guest Previews",
       IFNULL(t.guest_previews_90_days, 0) as "90 Days Guest Previews",
       IFNULL(t.guest_previews_24_months, 0) as "24 Months Guest Previews",
       IFNULL(t.unique_member_preview_30_days, 0) as "30 Days Member Previews",
       IFNULL(t.unique_member_preview_90_days, 0) as "90 Days Member Previews",
       IFNULL(t.unique_member_preview_24_months, 0) as "24 Months Member Previews",
       IFNULL(t.guest_shares_30_days, 0) as "30 Days Guest Shares",
       IFNULL(t.guest_shares_90_days, 0) as "90 Days Guest Shares",
       IFNULL(t.guest_shares_24_months, 0) as "24 Months Guest Shares",
       IFNULL(t.unique_member_shares_30_days, 0) as "30 Days Member Shares",
       IFNULL(t.unique_member_shares_90_days, 0) as "90 Days Member Shares",
       IFNULL(t.unique_member_shares_24_months, 0) as "24 Months Member Shares",
       --
       IFNULL(t.guest_clickthroughs_30_days, 0) as "30 Days Guest Click-throughs to Express",
       IFNULL(t.guest_clickthroughs_90_days, 0) as "90 Days Guest Click-throughs to Express",
       IFNULL(t.guest_clickthroughs_24_months, 0) as "24 Months Guest Click-throughs to Express",
       IFNULL(t.unique_member_clickthrough_30_days, 0) as "30 Days Member Click-throughs to Express",
       IFNULL(t.unique_member_clickthrough_90_days, 0) as "90 Days Member Click-throughs to Express",
       IFNULL(t.unique_member_clickthrough_24_months, 0) as "24 Months Member Click-throughs to Express",
       --
       IFNULL(t.fav_days_30, 0) as "30 Days Number of Times Favorited",
       IFNULL(t.fav_days_60, 0) as "60 Days Number of Times Favorited",
       IFNULL(t.fav_days_90, 0) as "90 Days Number of Times Favorited",
       IFNULL(t.fav_days_730, 0) as "24 Months of Times Favorited",
       IFNULL(t.fav_days_30_active, 0) as "30 Days Members Currently Favorited",
       IFNULL(t.fav_days_60_active, 0) as "60 Days Members Currently Favorited",
       IFNULL(t.fav_days_90_active, 0) as "90 Days Members Currently Favorited",
       IFNULL(t.fav_days_730_active, 0) as "24 Months Members Currently Favorited"
from edx.rpt_content_detail_teaching_resources t
left join edx.rpt_content_tr_priority_resource_id tm on tm.resourceID = t.resourceID
where t.isCurrent = 1
order by 8,1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17,18;
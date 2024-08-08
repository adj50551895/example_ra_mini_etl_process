drop table if exists user_gk.edex_member_segmentation_extract_1;

create table user_gk.edex_member_segmentation_extract_1 as
select a.memberId as memberguid,
       a.userguid as adobeguid,
       b.business_group,
       b.target_group, 
       b.email,
       b.country,
       c.state_province
from user_gk.member_guid_map a
inner join mdpd_target.mdpd_user_ww b on a.userguid=b.user_guid
left join mdpd_target.identity_composite_najp c on a.userguid=c.user_guid;
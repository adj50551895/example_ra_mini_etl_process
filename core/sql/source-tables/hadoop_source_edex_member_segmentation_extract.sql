select t.memberguid,
       t.adobeguid,
       -- t.member_created,
       t.business_group,
       t.target_group,
       t.email,
       t.country,
       t.state_province
from user_gk.edex_member_segmentation_extract_1 t
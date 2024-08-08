select t.memberguid,
       t.adobeguid,
       -- t.member_created,
       t.business_group,
       t.target_group,
       t.email,
       t.country,
       t.state_province
from ccanalytics.ra_edex_member_segmentation_extract_A t;
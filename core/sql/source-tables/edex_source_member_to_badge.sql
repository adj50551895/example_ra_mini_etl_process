select t.memberbadgeid as memberBadgeID,
       t.memberid as memberID,
       t.badgeid as badgeID,
       t.status as status,
       t.coursevanityurl as courseVanityURL,
       t.createdat as createdAt,
       t.createdby as createdBy,
       t.updatedat as updatedAt
from edex_members.MemberToBadge t
where t.badgeid = '8626c24f-f75a-4167-9861-24c4ebdbe8ca';
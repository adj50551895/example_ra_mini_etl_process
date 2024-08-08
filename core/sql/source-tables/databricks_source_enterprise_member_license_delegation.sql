select distinct t.member_guid,
       t.org_id,
       t.delegation_status
from enterprise.fact_enterprise_member_license_delegation t
join ccanalytics.ra_edex_member_guid_map me on me.userguid = t.member_guid
where t.market_segment = 'EDUCATION'
and t.is_valid = 'Y';
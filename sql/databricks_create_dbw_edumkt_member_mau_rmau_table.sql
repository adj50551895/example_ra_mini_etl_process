drop table if exists ccanalytics.dbw_edumkt_member_mau_rmau;

create table ccanalytics.dbw_edumkt_member_mau_rmau as 
select t.as_of_date,
       t.member_guid,
       mp.memberId,
       count(distinct if(split(t.last_ts,' ')[0] between date_add(cast(t.as_of_date as date),-27) and cast(t.as_of_date as date) and t.skucontracttypeprimary = 'cce', t.member_guid, null)) as mau,
       count(distinct if(t.skucontracttypeprimary = 'cce', t.original_guid, null)) as rmau,
       count(distinct if(t.original_guid is not null and t.skucontracttypeprimary = 'cce', t.original_guid, null)) as rmau1,
       count(distinct if((split(t.last_ts,' ')[0] between date_add(cast(t.as_of_date as date),-27) and cast(t.as_of_date as date)) and 
             split(t.first_ts,' ')[0] < date_add(cast(t.as_of_date as date),-27), t.member_guid, null)) as returning_mau
       ,count(*) as cnt_
from edumkt.edu_delegated_table_deduped2 t
join ccanalytics.ra_edex_member_guid_map mp on mp.userGuid = t.member_guid
-- where t.skuContractTypePrimary = 'CCE'
group by t.as_of_date,
         t.member_guid,
         mp.memberId;
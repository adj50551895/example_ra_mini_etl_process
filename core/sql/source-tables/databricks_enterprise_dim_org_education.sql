select t.org_id,
       t.org_name,
       t.jem_org_type,
       t.renga_org_type,
       t.market_segment,
       t.country,
       t.org_domain,
       t.is_parent,
       t.is_root_org,
       t.market_subsegment,
       t.esm_status,
       case when lower(trim(t.market_subsegment)) = 'k_12,non_profit' then 'K12'
            when lower(trim(t.market_subsegment)) = 'higher_ed,non_profit' then 'HED'
            when lower(trim(t.market_subsegment)) = 'higher_ed,k_12' then 'both'
            when lower(trim(t.market_subsegment)) = 'edu_k12' then 'K12'
            when lower(trim(t.market_subsegment)) = 'edu_hed,edu_k12' then 'both'
            when lower(trim(t.market_subsegment)) = 'edu,edu_hed,edu_k12' then 'both' 
            when lower(trim(t.market_subsegment)) = 'higher_ed' then 'HED'
            when lower(trim(t.market_subsegment)) = 'k_12' then 'K12'
            when lower(trim(t.market_subsegment)) = 'edu_hed' then 'HED'
            when lower(trim(t.market_subsegment)) = 'higher_ed,k_12,non_profit' then 'both'
            when lower(trim(t.market_subsegment)) = 'edu,edu_k12' then 'K12'
            when lower(trim(market_subsegment)) = 'edu,edu_hed' then 'HED'
            when lower(trim(market_subsegment)) = 'edu,edu_k12' then 'K12'
            else 'other'
       end as class_
from enterprise.dim_org t
where t.market_segment = 'EDUCATION';
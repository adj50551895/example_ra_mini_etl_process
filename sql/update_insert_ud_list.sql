insert into edx.ud_lookup
select m.id, 'US'
from edx.Member m
where m.countryCode='UD'
and m.email not like '%.in'
and m.email not like '%.nz'
and m.email not like '%.tw'
and m.email not like '%.au'
and m.email not like '%.jp'
and m.email not like '%.uk'
and m.email not like '%.ch'
and m.email not like '%.no'
and m.email not like '%.me'
and m.email not like '%.ca'
and m.email not like '%.de'
and m.email not like '%.nl'
and m.email not like '%.it'
and m.email not like '%.be'
and m.email not like '%.th'
and m.settings like '%en-us%'
and m.createdAt >= date('2023-01-08')
and not exists (select 1 from edx.ud_lookup l where l.memberId = m.id);
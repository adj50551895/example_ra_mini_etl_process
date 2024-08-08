select d.domain,
       d.school_type_desc,
       d.school_type,
       d.school_class,
       t.domain_class,
       t.nces_id
from edx.school_domain d
join edx.district_domains t on t.domain = d.domain
where d.school_class != t.domain_class;

-- insert into edx.school_domain
select distinct d.domain, 'school', 'school', d.domain_class                
from edx.district_domains d
where d.domain not in (select t.domain from edx.school_domain t);


drop table if exists ra.base_domains;
create table ra.base_domains
SELECT
  domain AS domen,
  SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(domain, '.', ','), ',', -1), ',',  1) AS subdomen1,
  SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(domain, '.', ','), ',', -2), ',',  1) AS subdomen2,
  SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(domain, '.', ','), ',', -3), ',',  1) AS subdomen3,
  SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(domain, '.', ','), ',', -4), ',',  1) AS subdomen4,
  SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(domain, '.', ','), ',', -5), ',',  1) AS subdomen5,
  LENGTH(domain) - LENGTH(REPLACE(domain, '.', '')) AS broj_tacaka,
  school_class
FROM edx.school_domain
WHERE school_class is not null;

-- 5
select t1.*,
       t2.school_class
from ra.base_domains t1
join ra.base_domains t2 on t2.subdomen1 = t1.subdomen1
                       and t2.subdomen2 = t1.subdomen2
                       and t2.subdomen3 = t1.subdomen3
                       and t2.subdomen4 = t1.subdomen4
                       and t2.subdomen5 = t1.subdomen5
where t1.school_class != t2.school_class
and t1.broj_tacaka = 5;

-- 4
select t1.*,
       t2.school_class
from ra.base_domains t1
join ra.base_domains t2 on t2.subdomen1 = t1.subdomen1
                       and t2.subdomen2 = t1.subdomen2
                       and t2.subdomen3 = t1.subdomen3
                       and t2.subdomen4 = t1.subdomen4
                       -- and t2.subdomen5 = t1.subdomen5
where t1.school_class != t2.school_class
and t1.broj_tacaka = 4;


-- 3
select t1.*,
       t2.domen as domen_2,
       t2.school_class as school_class_2,
       CONCAT(t1.subdomen3,'.',t1.subdomen2,'.',t1.subdomen1) as base_domain
from ra.base_domains t1
join ra.base_domains t2 on t2.subdomen1 = t1.subdomen1
                       and t2.subdomen2 = t1.subdomen2
                       and t2.subdomen3 = t1.subdomen3
                       -- and t2.subdomen4 = t1.subdomen4
                       -- and t2.subdomen5 = t1.subdomen5
where t1.school_class != t2.school_class
and t1.broj_tacaka = 3;

-- 2
drop table if exists ra.base_domains_temp;
create table ra.base_domains_temp
select t1.*,
       t2.domen as domen_2,
       t2.school_class as school_class_2,
       CONCAT(t1.subdomen3,'.',t1.subdomen2,'.',t1.subdomen1) as base_domain
from ra.base_domains t1
join ra.base_domains t2 on t2.subdomen1 = t1.subdomen1
                       and t2.subdomen2 = t1.subdomen2
                       -- and t2.subdomen3 = t1.subdomen3
                       -- and t2.subdomen4 = t1.subdomen4
                       -- and t2.subdomen5 = t1.subdomen5
where t1.school_class != t2.school_class
and t1.broj_tacaka = 2;

update edx.school_domain t set t.school_class = 'HED'
where 1=1
and school_type = 'school'
and concat(SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(domain, '.', ','), ',', -3), ',',  1),'.',
         SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(domain, '.', ','), ',', -2), ',',  1),'.',
		SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(domain, '.', ','), ',', -1), ',',  1))
in (
'adveti.ac.ae',
'alton.ac.uk',
'anglia.ac.uk',
'aru.ac.uk',
'aub.ac.uk',
'barnetsouthgate.ac.uk',
'bbk.ac.uk',
'bcot.ac.uk',
'bedford.ac.uk',
'bham.ac.uk',
'bhasvic.ac.uk',
'borderscollege.ac.uk',
'boston.ac.uk',
'bpc.ac.uk',
'bradford.ac.uk',
'bsfc.ac.uk',
'buckscollegegroup.ac.uk',
'burnley.ac.uk',
'calderdale.ac.uk',
'candi.ac.uk',
'cant-col.ac.uk',
'capitalccg.ac.uk',
'cavc.ac.uk',
'ccb.ac.uk',
'ccn.ac.uk',
'ccsw.ac.uk',
'centralbeds.ac.uk',
'chelmsford.ac.uk',
'chesterfield.ac.uk',
'chichester.ac.uk',
'chula.ac.th',
'citybathcoll.ac.uk',
'citylit.ac.uk',
'cityofbristol.ac.uk',
'cmcnet.ac.uk',
'cnwl.ac.uk',
'confetti.ac.uk',
'coulsdon.ac.uk',
'covcollege.ac.uk',
'coventrycollege.ac.uk',
'croydon.ac.uk',
'derby-college.ac.uk',
'don.ac.uk',
'dumgal.ac.uk',
'eastdurham.ac.uk',
'eastkent.ac.uk',
'esc.ac.uk',
'fife.ac.uk',
'gateshead.ac.uk',
'gbmc.ac.uk',
'glasgowclyde.ac.uk',
'gloscol.ac.uk',
'gsa.ac.uk',
'hal.ac.jp',
'highbury.ac.uk',
'its.ac.id',
'keele.ac.uk',
'kis.or.kr',
'kmutt.ac.th',
'leeds-art.ac.uk',
'lincoln.ac.uk',
'londonmet.ac.uk',
'lsa.ac.uk',
'ncclondon.ac.uk',
'ncl-coll.ac.uk',
'newport.ac.uk',
'ntu.ac.uk',
'nwu.ac.za',
'plymouth.ac.uk',
'psc.ac.uk',
'rave.ac.uk',
'smu.ac.uk',
'solihull.ac.uk',
'southwales.ac.uk',
'src.ac.uk',
'stir.ac.uk',
'students.ac.uk',
'uca.ac.uk',
'ucreative.ac.uk',
'uhi.ac.uk',
'uws.ac.uk',
'uwtsd.ac.uk',
'waikato.ac.nz',
'wilderness.com.au',
'wits.ac.za',
'wnc.ac.uk',
'wolvcoll.ac.uk'
)'
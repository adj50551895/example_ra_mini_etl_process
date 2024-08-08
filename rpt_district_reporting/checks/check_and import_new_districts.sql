-- importing file to ra.top_200_disticts_20230831

-- create table edx.district_domains_20230905 as
-- select * from edx.district_domains;

update ra.top_200_disticts_202300903 set District_Name= upper(District_Name);

-- update ra.top_200_disticts_20230831 set NCES = concat('0', NCES)
-- where length(NCES)<7
-- and NCES != '80';

select *
from edx.district_domains d
where d.district_id = 115;

-- 1 -- 3701140	DAVIDSON COUNTY SCHOOLS (4703180) -- ncesid was 4703180
delete from edx.district_domains d
where district_id = 113
and d.domain in ('mnps.org', 'mnpsk12.org');

update edx.district_domains d set d.top_200 = '3701140',
                                  d.top_100 = '3701140',
                                  d.nces_id = '3701140',
                                  d.high_rise = '3701140'
where district_id = 113;


-- 2 -- 0407750	DEER VALLEY UNIFIED DISTRICT (0400278) -- ncesid was 0400278
update edx.district_domains d set d.top_200 = '0407750',                                  
                                  d.nces_id = '0407750'
where district_id = 115;


-- 3 -- 0904320	STAMFORD SCHOOL DISTRICT (0) -- ncesid was 0
update edx.district_domains d set d.top_200 = '0904320',
                                  d.school_district_name = 'STAMFORD SCHOOL DISTRICT',
                                  d.nces_id = '0904320',
                                  d.pod = 'NE'
where district_id = 92;


update edx.district_domains set school_district_name = 'TUSTIN UNIFIED' where district_id = 180;

-- --------------------------------------------------
select  t.NCES,
		t.District_Name,
        d.nces_id,
        d.school_district_name,
        t.Territory,
        d.pod,
        d.top_200,
        t.Segment,
        d.sky_scraper,
        d.vsky,
        d.high_rise,
        count(*) as no_of_domains
from ra.top_200_disticts_202300903 t
left join edx.district_domains d on d.nces_id = t.NCES
-- where t.Territory != d.pod
group by t.NCES,
		 t.District_Name,
         d.nces_id,
         d.school_district_name,
         t.Territory,
         d.pod,
         d.top_200,
         t.Segment,
         d.sky_scraper,
         d.vsky,
         d.high_rise;

-- 1 top_200
update edx.district_domains d set d.top_200 = 0;
update edx.district_domains d set d.top_200 = d.nces_id
where d.nces_id in (select t.NCES from ra.top_200_disticts_202300903 t);

-- 2 vsky
update edx.district_domains d set d.vsky = 0;
update edx.district_domains d set d.vsky = d.nces_id
where d.nces_id in (select t.NCES from ra.top_200_disticts_202300903 t where t.Segment like '%V-Skies%');

-- 3 Skyscrapers
update edx.district_domains d set d.sky_scraper = 0;
update edx.district_domains d set d.sky_scraper = d.nces_id
where d.nces_id in (select t.NCES from ra.top_200_disticts_202300903 t where t.Segment like '%Skyscrapers%');

-- 4 high_rise
update edx.district_domains d set d.high_rise = 0;
update edx.district_domains d set d.high_rise = d.nces_id
where d.nces_id in (select t.NCES from ra.top_200_disticts_202300903 t where t.Segment like '%High%Rise%');


select distinct t.Segment
from ra.top_200_disticts_202300903 t
where t.Segment like '%High%Rise%'

select distinct
       t.nces_id,
       t.school_district_name,        
       t.top_500,
       t.top_200,
       t.top_100,
       t.pod,
       t.sky_scraper,
       t.vsky,
       t.high_rise       
from edx.district_domains t
where t.nces_id != 0;

select *
from edx.district_domains
where nces_id like '%904320%'



-- Verified discrepancies
-- 0629550	PAJARO VALLEY UNIFIED	0629550	PALM SPRINGS UNIFIED SCHOOL DISTRICT - OCT
-- 4900120	CACHE DISTRICT	4900120	UEN - CACHE
-- 0404970	NEBO DISTRICT	0404970	MESA USD CAREER AND TECHNICAL EDUCATION
-- 0405930	STOCKTON UNIFIED	0405930	PARADISE VALLEY UNIFIED SCHOOL DISTRICT
-- 0612330	JEFFERSON COUNTY SCHOOL DISTRICT NO. R-1	0612330	ELK GROVE UNIFIED SCHOOL DISTRICT
-- 0200180	BEAVERTON SD 48J	0200180	ANCHORAGE SCHOOL DISTRICT
-- 0628650	PALM SPRINGS UNIFIED	0628650	ORANGE UNIFIED SCHOOL DISTRICT
-- 0629490	PARADISE VALLEY UNIFIED DISTRICT (4241)	0629490	PAJARO VALLEY UNIFIED SCHOOL DISTRICT
-- 0638010	KENT SCHOOL DISTRICT	0638010	STOCKTON UNIFIED SCHOOL DISTRICT -2016
-- 0640150	VISALIA UNIFIED	0640150	INFORMATION TECHNOLOGY
-- 0641160	ALBUQUERQUE PUBLIC SCHOOLS	0641160	VISALIA UNIFIED SCHOOL DISTRICT
-- 0804800	JORDAN DISTRICT	0804800	JEFFERSON COUNTY SCHOOL DISTRICT R-1
-- 3501500	MESA UNIFIED DISTRICT (4235)	3501500	LAS CRUCES SCHOOL DISTRICT
-- 4101920	CANYONS DISTRICT	4101920	BEAVERTON SCHOOL DISTRICT
-- 4900420	LAS CRUCES PUBLIC SCHOOLS	4900420	UEN - JORDAN
-- 4900630	ORANGE UNIFIED	4900630	UEN - NEBO
-- 4900870	SEATTLE SCHOOL DISTRICT NO. 1	4900870	UEN - SALT LAKE
-- 4901050	TUSTIN UNIFIED	4901050	UEN - TOOELE DISTRICT
-- 5307710	SPOKANE SCHOOL DISTRICT	5307710	SEATTLE SCHOOL DISTRICT
-- 5308250	TOOELE DISTRICT	5308250	SPOKANE PUBLIC SCHOOLS
-- 4900142	DAVIS DISTRICT	4900142	UEN - CANYONS
-- 4900210	ELK GROVE UNIFIED	4900210	UEN - DAVIS DISTRICT

-- Change District Name
4502700	LEXINGTON 01	4502700	LEXINGTON COUNTY SCHOOL DISTRICT ONE
4502820	LEXINGTON 05	4502820	LEXINGTON 05
80	NYC DOE	80	NYCDOE


-- --------------------------------------------------------------
select *
from edx.district_domains d
where d.nces_id = '0628650';

select *
from ra.top_200_disticts_202300903
where nces = '1300120';


-- --------------------------------------------------------------
select *
from edx.district_domains d order by 1,2,3;

select *
from edx.top_200_disticts_202300903 d order by 1,2,3;
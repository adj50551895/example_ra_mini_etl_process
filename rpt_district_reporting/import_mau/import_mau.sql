-- select sum(mau), sum(rmau_1), sum(rmau_2), sum(resurrected_mau) from edx.rpt_district_import_mau;

-- select * from ra.tmp_district_report_mau_temp
-- find and remove: ,
-- find and remove: "



-- select * from ra.tmp_district_report_mau_temp

delete from ra.tmp_district_report_mau_temp where nces_id_district like ' - ';

update ra.tmp_district_report_mau_temp set nces_id_district = trim(upper(nces_id_district));

drop table if exists ra.tmp_district_report_mau;
create table ra.tmp_district_report_mau as
select max(t.date) as date, -- t.date,
       t.nces_id_district,
       max(t.mau) as mau,
       max(rmau_1) as rmau_1
from ra.tmp_district_report_mau_temp t
group by month(t.date),-- t.calendar_date, -- t.date,
         t.nces_id_district;

-- select * 
delete from edx.rpt_district_import_mau t 
where t.fiscal_yr_and_per in (
select dd.fiscal_yr_and_per 
from ra.tmp_district_report_mau t1
join edx.hana_dim_date dd on dd.calendar_date = t1.date);


-- drop table if exists edx.rpt_district_import_mau;
-- create table edx.rpt_district_import_mau as
insert into edx.rpt_district_import_mau
select distinct -- dd.calendar_date, 
       dd.fiscal_yr_and_per,
       t.nces_id_district as "nces_id_district",
       SUBSTRING_INDEX(t.nces_id_district,' - ', 1) as "nces_id",
       t.mau,
       t.rmau_1,
       0 as rmau_2,-- t.rmau_2,
       0 as resurrected_mau,-- t.resurrected_mau as resurrected_mau,
       d.district_id,
       sysdate() as import_time
from ra.tmp_district_report_mau t
join edx.hana_dim_date dd on dd.calendar_date = t.date
left join edx.district_domains d on d.nces_id = SUBSTRING_INDEX(t.nces_id_district,' - ', 1);

drop table ra.tmp_district_report_mau_temp;
drop table ra.tmp_district_report_mau;
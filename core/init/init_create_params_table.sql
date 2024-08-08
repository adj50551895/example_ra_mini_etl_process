drop table if exists edx.rpt_params;
create table edx.rpt_params (
report_name char(100) CHARACTER SET ascii,
param_name char(50) CHARACTER SET ascii,
param_value char(50) CHARACTER SET ascii,
period char(50) CHARACTER SET ascii,
dateUpdated char(36) CHARACTER SET ascii
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into edx.rpt_params
select 'rpt_content_detail' as report_name, 'course_min_date' as param_name, '2021-12-03' as param_value, 'weekly' as period;
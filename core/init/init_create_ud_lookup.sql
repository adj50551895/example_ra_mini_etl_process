drop table if exists edx.ud_lookup;
create table edx.hana_dim_date (
memberId CHAR(50) NOT NULL PRIMARY KEY,
countryCode CHAR(5)
);
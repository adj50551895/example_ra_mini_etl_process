select 
cast(createdAt as Date) as ' Member Created At'
,count(distinct flag_table.id) as 'Member Singups'
-- ,count(distinct flag_table.edu_email) as 'School Email Signups'
,count(distinct flag_table.edu_email_1) as 'School Email Signups'
-- ,count(distinct flag_table.edu_email)/COUNT(distinct flag_table.id) as 'Average school Email Signups'
,count(distinct flag_table.edu_email_1)/COUNT(distinct flag_table.id) as 'Average school Email Signups'
,count(distinct flag_table.us_signups) as 'US Singups'
,COUNT(distinct flag_table.us_signups)/COUNT(distinct flag_table.id) as ' % of US Signups compared to total Signups'
-- ,COUNT(distinct flag_table.us_edu_signups) as 'US School Email Signups'
,COUNT(distinct flag_table.us_edu_signups_1) as 'US School Email Signups'
-- ,COUNT(distinct flag_table.us_edu_signups)/COUNT(distinct flag_table.edu_email) as '% of US School Email Signups compared to total School Email Signups'
,COUNT(distinct flag_table.us_edu_signups_1)/COUNT(distinct flag_table.edu_email_1) as '% of US School Email Signups compared to total School Email Signups'
-- ,COUNT(distinct flag_table.us_edu_signups)/COUNT(distinct flag_table.us_signups) as 'Average US School Email  Signups  compared  to US Signups'
,COUNT(distinct flag_table.us_edu_signups_1)/COUNT(distinct flag_table.us_signups) as 'Average US School Email  Signups  compared  to US Signups'
,COUNT(distinct flag_table.row_signups) as ' ROW Signups'
,COUNT(distinct flag_table.row_signups)/COUNT(distinct flag_table.id) as '% of ROW Signups compared to total Signups'
-- ,COUNT(distinct flag_table.row_edu_signups) as ' Row School Email Signups'
,COUNT(distinct flag_table.row_edu_signups_1) as ' Row School Email Signups'
-- ,COUNT(distinct flag_table.row_edu_signups)/COUNT(distinct flag_table.id) as '% of ROW School Email Signups compared to total School Email Signups'
,COUNT(distinct flag_table.row_edu_signups_1)/COUNT(distinct flag_table.id) as '% of ROW School Email Signups compared to total School Email Signups'
-- ,COUNT(distinct flag_table.row_edu_signups)/COUNT(distinct flag_table.row_signups) as 'Average ROW School Email  Signups  compared  to ROW Signups'
,COUNT(distinct flag_table.row_edu_signups_1)/COUNT(distinct flag_table.row_signups) as 'Average ROW School Email  Signups  compared  to ROW Signups'
--
,COUNT(distinct flag_table.us_edu_signups_k12_1)/COUNT(distinct flag_table.us_signups) as 'Average US K12 School Email Signups  compared  to US Signups'
,COUNT(distinct flag_table.us_edu_signups_hed_1)/COUNT(distinct flag_table.us_signups) as 'Average US HED School Email Signups  compared  to US Signups'
FROM
(SELECT m.id,
        cast(m.createdAt as Date) as createdAt,
        m.email,
        m.countryCode,
        CASE WHEN m.countryCode = 'US' THEN m.id END AS us_signups,
        CASE WHEN m.countryCode != 'US' THEN m.id END as row_signups,
        CASE WHEN (LOWER(m.email) LIKE '%.edu%' OR LOWER(m.email) LIKE '%k12%') THEN m.id END as edu_email,
        CASE WHEN (LOWER(m.email)LIKE'%.edu%' OR LOWER(m.email) LIKE'%k12%') AND m.countryCode = 'US' THEN m.id END as us_edu_signups,
        CASE WHEN (LOWER(m.email) LIKE'%.edu%' OR LOWER(m.email) LIKE'%k12%') AND m.countryCode != 'US' THEN m.id END as row_edu_signups,
        CASE WHEN t.school_type ='school' THEN m.id END as edu_email_1,
        CASE WHEN (t.school_type ='school' AND t.school_class = 'K12') THEN m.id END as edu_email_k12_1,
        CASE WHEN (t.school_type ='school' AND t.school_class = 'HED') THEN m.id END as edu_email_hed_1,
        CASE WHEN (t.school_type ='school' AND m.countryCode = 'US') THEN m.id END as us_edu_signups_1,
        CASE WHEN (t.school_type ='school' AND m.countryCode = 'US' AND t.school_class = 'K12') THEN m.id END as us_edu_signups_k12_1,
        CASE WHEN (t.school_type ='school' AND m.countryCode = 'US' AND t.school_class = 'HED') THEN m.id END as us_edu_signups_hed_1,
        CASE WHEN (t.school_type ='school' AND m.countryCode != 'US') THEN m.id END as row_edu_signups_1,
        CASE WHEN (t.school_type ='school' AND m.countryCode != 'US' AND t.school_class = 'K12') THEN m.id END as row_edu_signups_k12_1,
        CASE WHEN (t.school_type ='school' AND m.countryCode != 'US' AND t.school_class = 'HED') THEN m.id END as row_edu_signups_hed_1,
        t.school_type,
        t.school_class
FROM edx.Member m
left join edx.school_domain t on t.domain = TRIM(LOWER(SUBSTRING_INDEX(m.email, '@', - 1)))
) as flag_table
WHERE createdAt >= date('2023-01-01')
group by createdAt
ORDER BY createdAt;


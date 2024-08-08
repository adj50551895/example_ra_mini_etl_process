SELECT a.id,
       a.i18nCategory,
       a.i18nLabel, a.urlLabel,
       case when a.i18nLabel = 'i18n.academicLevel.Higher_Education' then 'HED'
            when a.i18nLabel = 'i18n.academicLevel.All_Ages' then 'ALL_AGES'
            else 'K12' end as eduLevel
FROM edex_metadata.AcademicLevel a
WHERE a.status = 'active';
SELECT st.id,
       st.i18nLabel,
       case
           when st.i18nLabel = 'i18n.school_type.University' then 'HED'
           when st.i18nLabel = 'i18n.school_type.4_Year_College' then 'HED'
           when st.i18nLabel = 'i18n.school_type.2_Year_College' then 'HED'
           when st.i18nLabel = 'i18n.school_type.Career_and_Tech_Ed' then 'HED'
           when st.i18nLabel = 'i18n.school_type.Other' then 'Other'
           when st.i18nLabel = 'i18n.school_type.Adult_Learning_Institution' then 'AdultEducation'
           else 'K12'
        end as schoolCategory
FROM edex_metadata.SchoolType st
WHERE st.status = 'active';
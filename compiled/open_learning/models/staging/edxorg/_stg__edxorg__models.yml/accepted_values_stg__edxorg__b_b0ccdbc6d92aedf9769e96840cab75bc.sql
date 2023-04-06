with all_values as (

    select
        user_highest_education as value_field
        , count(*) as n_records

    from dev.main_staging.stg__edxorg__bigquery__mitx_user_info_combo
    group by user_highest_education

)

select *
from all_values
where value_field not in (
    'Doctorate'
    , 'Master''s or professional degree'
    , 'Bachelor''s degree'
    , 'Associate degree'
    , 'Secondary/high school'
    , 'Junior secondary/junior high/middle school'
    , 'Elementary/primary school'
    , 'No formal education'
    , 'Other education'
    , 'Doctorate in science or engineering'
    , 'Doctorate in another field'
    , ''
    , 'None'
)

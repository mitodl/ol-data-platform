with all_values as (

    select
        user_highest_education as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxpro__app__postgres__users_profile
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
    , ''
)

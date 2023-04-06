with all_values as (

    select
        user_years_experience as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxpro__users
    group by user_years_experience

)

select *
from all_values
where value_field not in (
    'Less than 2 years'
    , '2-5 years'
    , '6 - 10 years'
    , '11 - 15 years'
    , '16 - 20 years'
    , 'More than 20 years'
    , 'Prefer not to say'
)

with all_values as (

    select
        user_gender as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxonline__users
    group by user_gender

)

select *
from all_values
where value_field not in (
    'Male', 'Female', 'Transgender', 'Non-binary/non-conforming', 'Other/Prefer Not to Say'
)

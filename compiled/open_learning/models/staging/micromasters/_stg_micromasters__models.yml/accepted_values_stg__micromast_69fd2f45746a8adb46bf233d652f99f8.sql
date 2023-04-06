with all_values as (

    select
        user_account_privacy as value_field
        , count(*) as n_records

    from dev.main_staging.stg__micromasters__app__postgres__profiles_profile
    group by user_account_privacy

)

select *
from all_values
where value_field not in (
    'Public to everyone', 'Public to logged in users', 'Private'
)

with all_values as (

    select
        user_auth_provider as value_field
        , count(*) as n_records

    from dev.main_staging.stg__micromasters__app__postgres__auth_usersocialauth
    group by user_auth_provider

)

select *
from all_values
where value_field not in (
    'edxorg', 'mitxonline'
)

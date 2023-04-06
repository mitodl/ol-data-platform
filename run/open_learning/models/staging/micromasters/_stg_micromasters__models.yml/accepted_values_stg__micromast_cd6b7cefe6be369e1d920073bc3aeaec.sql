select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            user_auth_provider as value_field
            , count(*) as n_records

        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__auth_usersocialauth
        group by user_auth_provider

    )

    select *
    from all_values
    where
        value_field not in (
            'edxorg', 'mitxonline'
        )




) as dbt_internal_test

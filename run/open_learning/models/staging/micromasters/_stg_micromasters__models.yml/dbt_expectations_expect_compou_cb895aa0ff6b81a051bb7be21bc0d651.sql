select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            user_username
            , user_auth_provider
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__auth_usersocialauth
        where
            1 = 1
            and
            not (
                user_username is null
                and user_auth_provider is null

            )



        group by
            user_username, user_auth_provider
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

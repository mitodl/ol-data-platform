select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            user_gender as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__users_profile
        group by user_gender

    )

    select *
    from all_values
    where
        value_field not in (
            'Male', 'Female', 'Other/Prefer Not to Say', ''
        )




) as dbt_internal_test

select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            user_years_experience as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__users_profile
        group by user_years_experience

    )

    select *
    from all_values
    where
        value_field not in (
            'Less than 2 years'
            , '2-5 years'
            , '6 - 10 years'
            , '11 - 15 years'
            , '16 - 20 years'
            , 'More than 20 years'
            , 'Prefer not to say'
        )




) as dbt_internal_test

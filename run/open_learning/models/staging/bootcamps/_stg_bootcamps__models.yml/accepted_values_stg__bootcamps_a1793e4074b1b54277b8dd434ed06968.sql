select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            user_company_size as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__profiles_profile
        group by user_company_size

    )

    select *
    from all_values
    where
        value_field not in (
            'Small/Start-up (1+ employees)'
            , 'Small/Home office (1-9 employees)'
            , 'Small (10-99 employees)'
            , 'Small to medium-sized (100-999 employees)'
            , 'Medium-sized (1000-9999 employees)'
            , 'Large Enterprise (10,000+ employees)'
            , 'Other (N/A or Don''t know)'
        )




) as dbt_internal_test

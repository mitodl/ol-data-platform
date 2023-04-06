select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            flexiblepriceapplication_status as value_field
            , count(*) as n_records

        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepriceapplication
        group by flexiblepriceapplication_status

    )

    select *
    from all_values
    where
        value_field not in (
            'approved', 'auto-approved', 'created', 'pending-manual-approval', 'denied', 'reset'
        )




) as dbt_internal_test

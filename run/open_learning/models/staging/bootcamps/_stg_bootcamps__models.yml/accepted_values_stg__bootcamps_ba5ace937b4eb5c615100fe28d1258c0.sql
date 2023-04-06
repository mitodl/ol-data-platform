select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            order_state as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__ecommerce_order
        group by order_state

    )

    select *
    from all_values
    where
        value_field not in (
            'fulfilled', 'failed', 'created', 'refunded'
        )




) as dbt_internal_test

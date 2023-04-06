select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            discount_type as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_discount
        group by discount_type

    )

    select *
    from all_values
    where
        value_field not in (
            'percent-off', 'dollars-off', 'fixed-price'
        )




) as dbt_internal_test

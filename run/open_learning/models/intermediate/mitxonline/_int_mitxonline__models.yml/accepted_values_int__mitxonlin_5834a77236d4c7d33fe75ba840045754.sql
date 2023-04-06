select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            transaction_type as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__ecommerce_transaction
        group by transaction_type

    )

    select *
    from all_values
    where
        value_field not in (
            'refund', 'payment'
        )




) as dbt_internal_test

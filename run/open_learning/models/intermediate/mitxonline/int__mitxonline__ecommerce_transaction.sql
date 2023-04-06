create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__ecommerce_transaction__dbt_tmp

as (
    with transactions as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_transaction
    )

    select
        transactions.transaction_id
        , transactions.transaction_amount
        , transactions.order_id
        , transactions.transaction_created_on
        , transactions.transaction_readable_identifier
        , transactions.transaction_type
    from transactions
);

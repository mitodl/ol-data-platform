create table ol_data_lake_production.ol_warehouse_production_intermediate.int__bootcamps__ecommerce_receipt__dbt_tmp

as (
    with receipts as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__ecommerce_receipt
    )

    select
        receipt_id
        , receipt_created_on
        , receipt_updated_on
        , receipt_data
        , order_id
    from receipts
);

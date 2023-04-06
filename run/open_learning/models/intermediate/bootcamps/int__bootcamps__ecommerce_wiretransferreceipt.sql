create table ol_data_lake_production.ol_warehouse_production_intermediate.int__bootcamps__ecommerce_wiretransferreceipt__dbt_tmp

as (
    with receipts as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__ecommerce_wiretransferreceipt
    )

    select
        wiretransferreceipt_id
        , wiretransferreceipt_created_on
        , wiretransferreceipt_updated_on
        , wiretransferreceipt_data
        , order_id
    from receipts
);

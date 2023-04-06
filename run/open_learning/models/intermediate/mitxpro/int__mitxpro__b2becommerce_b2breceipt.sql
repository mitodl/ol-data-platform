create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__b2becommerce_b2breceipt__dbt_tmp

as (
    with b2breceipts as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__b2becommerce_b2breceipt
    )

    select
        b2breceipt_id
        , b2breceipt_created_on
        , b2breceipt_updated_on
        , b2breceipt_data
        , b2border_id
    from b2breceipts
);

create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__ecommerce_basketdiscount__dbt_tmp

as (
    with source as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_basketdiscount
    )

    select
        basketdiscount_id
        , basketdiscount_created_on
        , basketdiscount_updated_on
        , user_id
        , basketdiscount_applied_on
        , basket_id
        , discount_id
    from source
);

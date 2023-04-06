create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__ecommerce_basketitem__dbt_tmp

as (
    with source as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_basketitem
    )

    select
        basketitem_id
        , basketitem_quantity
        , basket_id
        , basketitem_created_on
        , product_id
        , basketitem_updated_on
    from source
);

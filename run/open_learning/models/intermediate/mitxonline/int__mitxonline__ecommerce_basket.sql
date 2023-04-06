create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__ecommerce_basket__dbt_tmp

as (
    with source as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_basket
    )

    select
        basket_id
        , user_id
        , basket_created_on
        , basket_updated_on
    from source
);

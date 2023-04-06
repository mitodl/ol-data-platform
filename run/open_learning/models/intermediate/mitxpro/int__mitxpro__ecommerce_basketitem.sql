create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_basketitem__dbt_tmp

as (
    with basketitem as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_basketitem
    )

    , basket as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_basket
    )

    select
        basketitem.basketitem_id
        , basketitem.basketitem_quantity
        , basketitem.basket_id
        , basketitem.basketitem_created_on
        , basketitem.product_id
        , basketitem.basketitem_updated_on
        , basketitem.programrun_id
        , basket.user_id
    from basketitem
    inner join basket on basketitem.basket_id = basket.basket_id
);

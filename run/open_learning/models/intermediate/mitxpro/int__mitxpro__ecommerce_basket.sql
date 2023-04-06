create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_basket__dbt_tmp

as (
    with basket as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_basket
    )

    , couponbasket as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_couponbasket
    )

    select
        basket.basket_id
        , basket.user_id
        , basket.basket_created_on
        , basket.basket_updated_on
        , couponbasket.coupon_id
    from basket
    left join couponbasket on couponbasket.basket_id = basket.basket_id
);

create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_couponproduct__dbt_tmp

as (
    with couponproduct as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_couponproduct
    )

    select
        couponproduct_id
        , product_id
        , coupon_id
        , couponproduct_created_on
        , couponproduct_updated_on
        , programrun_id
    from couponproduct
);

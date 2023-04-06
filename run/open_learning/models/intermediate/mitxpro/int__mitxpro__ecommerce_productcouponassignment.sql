create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_productcouponassignment__dbt_tmp

as (
    with productcouponassignment as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_productcouponassignment
    )

    , couponproduct as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_couponproduct
    )


    select
        productcouponassignment.productcouponassignment_id
        , productcouponassignment.productcouponassignment_message_status_updated_on
        , productcouponassignment.productcouponassignment_is_redeemed
        , productcouponassignment.productcouponassignment_message_status
        , productcouponassignment.productcouponassignment_email
        , productcouponassignment.productcouponassignment_original_email
        , productcouponassignment.productcouponassignment_created_on
        , productcouponassignment.productcouponassignment_updated_on
        , couponproduct.coupon_id
        , couponproduct.product_id
        , couponproduct.programrun_id
    from productcouponassignment
    inner join couponproduct on couponproduct.couponproduct_id = productcouponassignment.couponproduct_id
);

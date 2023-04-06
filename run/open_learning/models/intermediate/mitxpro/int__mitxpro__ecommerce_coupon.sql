create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_coupon__dbt_tmp

as (
    with coupon as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_coupon
    )

    , couponpayment as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_couponpayment
    )

    select
        coupon.coupon_id
        , coupon.coupon_code
        , couponpayment.couponpayment_name
        , coupon.coupon_is_active
        , coupon.coupon_applies_to_future_runs
        , coupon.coupon_is_global
        , coupon.coupon_updated_on
        , coupon.coupon_created_on
    from coupon
    inner join couponpayment on couponpayment.couponpayment_id = coupon.couponpayment_id
);

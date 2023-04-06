create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_productcouponassignment__dbt_tmp

as (
    with source as (

        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__ecommerce_productcouponassignment

    )

    , renamed as (

        select
            id as productcouponassignment_id
            , bulk_assignment_id as bulkcouponassignment_id
            , product_coupon_id as couponproduct_id
            , redeemed as productcouponassignment_is_redeemed
            , message_status as productcouponassignment_message_status
            , email as productcouponassignment_email
            , original_email as productcouponassignment_original_email
            ,
            to_iso8601(from_iso8601_timestamp(message_status_date))
            as productcouponassignment_message_status_updated_on
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as productcouponassignment_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as productcouponassignment_updated_on

        from source

    )

    select * from renamed
);

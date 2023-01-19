with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_productcouponassignment') }}

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
        , {{ cast_timestamp_to_iso8601('message_status_date') }} as productcouponassignment_message_status_updated_on
        , {{ cast_timestamp_to_iso8601('created_on') }} as productcouponassignment_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as productcouponassignment_updated_on

    from source

)

select * from renamed

with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__b2b_ecommerce_b2bcoupon') }}

)

, renamed as (

    select
        id as b2bcoupon_id
        , company_id
        , product_id
        , enabled as b2bcoupon_is_enabled
        , discount_percent as b2bcoupon_discount_percent
        , name as b2bcoupon_name
        , reusable as b2bcoupon_is_reusable
        , coupon_code as b2bcoupon_coupon_code
        , {{ cast_timestamp_to_iso8601('updated_on') }} as b2bcoupon_updated_on
        , {{ cast_timestamp_to_iso8601('created_on') }} as b2bcoupon_created_on
        , {{ cast_timestamp_to_iso8601('expiration_date') }} as b2bcoupon_expires_on
        , {{ cast_timestamp_to_iso8601('activation_date') }} as b2bcoupon_activated_on
    from source
)

select * from renamed

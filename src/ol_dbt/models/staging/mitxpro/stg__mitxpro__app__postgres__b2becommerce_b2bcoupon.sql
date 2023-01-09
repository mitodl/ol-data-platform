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
        , to_iso8601(from_iso8601_timestamp(updated_on)) as b2bcoupon_updated_on
        , to_iso8601(from_iso8601_timestamp(created_on)) as b2bcoupon_created_on
        , to_iso8601(from_iso8601_timestamp(expiration_date)) as b2bcoupon_expires_on
        , to_iso8601(from_iso8601_timestamp(activation_date)) as b2bcoupon_activated_on
    from source
)

select * from renamed

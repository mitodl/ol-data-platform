with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_couponversion') }}

)

, renamed as (
    select
        id as couponversion_id
        , coupon_id
        , payment_version_id as couponpaymentversion_id
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as couponversion_updated_on
        ,{{ cast_timestamp_to_iso8601('created_on') }} as couponversion_created_on
    from source
)

select * from renamed

with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_coupon') }}

)

, renamed as (
    select
        id as coupon_id
        , coupon_code
        , payment_id as couponpayment_id
        , enabled as coupon_is_active
        , include_future_runs as coupon_applies_to_future_runs
        , is_global as coupon_is_global
        , to_iso8601(from_iso8601_timestamp(updated_on)) as coupon_updated_on
        , to_iso8601(from_iso8601_timestamp(created_on)) as coupon_created_on
    from source
)

select * from renamed

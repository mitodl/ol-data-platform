with source as (

    {{ live_raw_rows('raw__micromasters__app__postgres__ecommerce_redeemedcoupon') }}

)

, renamed as (

    select
        id as redeemedcoupon_id
        , order_id
        , coupon_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as redeemedcoupon_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as redeemedcoupon_updated_on

    from source

)

select * from renamed

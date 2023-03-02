with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__ecommerce_usercoupon') }}

)

, renamed as (

    select
        id as usercoupon_id
        , user_id
        , coupon_id
        , {{ cast_timestamp_to_iso8601('created_on') }} as usercoupon_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as usercoupon_updated_on
    from source

)

select * from renamed

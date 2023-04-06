with source as (

    select * from dev.main_raw.raw__micromasters__app__postgres__ecommerce_usercoupon

)

, renamed as (

    select
        id as usercoupon_id
        , user_id
        , coupon_id
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as usercoupon_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as usercoupon_updated_on
    from source

)

select * from renamed

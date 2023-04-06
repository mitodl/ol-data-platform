with source as (

    select * from dev.main_raw.raw__micromasters__app__postgres__ecommerce_coupon

)

, renamed as (

    select
        id as coupon_id
        , coupon_code
        , object_id as coupon_object_id
        , amount_type as coupon_amount_type
        , amount as coupon_amount
        , enabled as coupon_is_active
        , content_type_id as contenttype_id
        , invoice_id as couponinvoice_id
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as coupon_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as coupon_updated_on
        ,
        to_iso8601(from_iso8601_timestamp(activation_date))
        as coupon_activated_on
        ,
        to_iso8601(from_iso8601_timestamp(expiration_date))
        as coupon_expires_on

    from source

)

select * from renamed

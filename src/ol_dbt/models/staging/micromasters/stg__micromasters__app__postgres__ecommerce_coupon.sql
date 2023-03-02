with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__ecommerce_coupon') }}

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
        , {{ cast_timestamp_to_iso8601('created_on') }} as coupon_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as coupon_updated_on
        , {{ cast_timestamp_to_iso8601('activation_date') }} as coupon_activated_on
        , {{ cast_timestamp_to_iso8601('expiration_date') }} as coupon_expires_on

    from source

)

select * from renamed

with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__ecommerce_coupon') }}

)

, renamed as (

    select
        id as coupon_id
        , coupon_code
        , coupon_type
        , object_id as coupon_object_id
        , amount_type as coupon_amount_type
        , cast(amount as decimal(38, 2)) as coupon_amount
        , enabled as coupon_is_active
        , content_type_id as contenttype_id
        , invoice_id as couponinvoice_id
        , case
            when amount_type = 'fixed-discount'
                then concat(format('%.2f', amount), ' off')
            when amount_type = 'fixed-price'
                then concat('Fixed Price: ', format('%.2f', amount))
            when amount_type = 'percent-discount'
                then concat(format('%.2f', amount * 100), '% off')
        end as coupon_discount_amount_text
        ,{{ cast_timestamp_to_iso8601('created_on') }} as coupon_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as coupon_updated_on
        ,{{ cast_timestamp_to_iso8601('activation_date') }} as coupon_activated_on
        ,{{ cast_timestamp_to_iso8601('expiration_date') }} as coupon_expires_on

    from source

)

select * from renamed

with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_couponpaymentversion') }}

)

, renamed as (

    select
        id as couponpaymentversion_id
        , company_id
        , num_coupon_codes as couponpaymentversion_num_coupon_codes
        , coupon_type as couponpaymentversion_coupon_type
        , amount as couponpaymentversion_discount_amount
        , max_redemptions_per_user as couponpaymentversion_max_redemptions_per_user
        , payment_id as couponpayment_id
        , automatic as couponpaymentversion_is_automatic
        , payment_type as couponpaymentversion_discount_source
        , payment_transaction as couponpaymentversion_payment_transaction
        , tag as couponpaymentversion_tag
        , discount_type as couponpaymentversion_discount_type
        , max_redemptions as couponpaymentversion_max_redemptions
        , case
            when discount_type = 'dollars-off'
                then concat('$', format('%.2f', amount), ' off')
            when discount_type = 'percent-off'
                then concat(format('%.2f', amount * 100), '% off')
        end as couponpaymentversion_discount_amount_text
        ,{{ cast_timestamp_to_iso8601('expiration_date') }} as couponpaymentversion_expires_on
        ,{{ cast_timestamp_to_iso8601('activation_date') }} as couponpaymentversion_activated_on
        ,{{ cast_timestamp_to_iso8601('created_on') }} as couponpaymentversion_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as couponpaymentversion_updated_on

    from source

)

select * from renamed

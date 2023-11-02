with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_discount') }}

)

, renamed as (

    select
        id as discount_id
        , amount as discount_amount
        , discount_code
        , discount_type
        , max_redemptions as discount_max_redemptions
        , redemption_type as discount_redemption_type
        , payment_type as discount_source
        , case
            when discount_type = 'percent-off'
                then concat(cast(amount as varchar), '% off')
            when discount_type = 'dollars-off'
                then concat('$', cast(amount as varchar), ' off')
            when discount_type = 'fixed-price'
                then concat('Fixed Price: ', cast(amount as varchar))
        end as discount_amount_text
        ,{{ cast_timestamp_to_iso8601('created_on') }} as discount_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as discount_updated_on
        ,{{ cast_timestamp_to_iso8601('activation_date') }} as discount_activated_on
        ,{{ cast_timestamp_to_iso8601('expiration_date') }} as discount_expires_on

    from source

)

select * from renamed

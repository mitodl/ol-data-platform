with source as (

    select *
    from
        {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_discount') }}

)

, renamed as (

    select
        id as discount_id
        , amount as discount_amount
        , discount_code
        , discount_type
        , max_redemptions as discount_max_redemptions
        , redemption_type as discount_redemption_type
        , for_flexible_pricing as discount_is_for_flexible_pricing
        , to_iso8601(from_iso8601_timestamp(created_on)) as discount_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as discount_updated_on
        , to_iso8601(
            from_iso8601_timestamp(activation_date)
        ) as discount_activated_on
        , to_iso8601(
            from_iso8601_timestamp(expiration_date)
        ) as discount_expires_on

    from source

)

select * from renamed

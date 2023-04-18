with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_discountredemption') }}

)

, renamed as (

    select
        id as discountredemption_id
        , redeemed_by_id as user_id
        , redeemed_order_id as order_id
        , redeemed_discount_id as discount_id
        ,{{ cast_timestamp_to_iso8601('redemption_date') }} as discountredemption_timestamp

    from source

)

select * from renamed

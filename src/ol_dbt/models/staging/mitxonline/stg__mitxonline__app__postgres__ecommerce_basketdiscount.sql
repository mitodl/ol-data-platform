with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_basketdiscount') }}

)

, renamed as (

    select
        id as basketdiscount_id
        , redeemed_by_id as user_id
        , redeemed_basket_id as basket_id
        , redeemed_discount_id as discount_id
        , {{ cast_timestamp_to_iso8601('redemption_date') }} as basketdiscount_applied_on
        , {{ cast_timestamp_to_iso8601('created_on') }} as basketdiscount_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as basketdiscount_updated_on

    from source

)

select * from renamed

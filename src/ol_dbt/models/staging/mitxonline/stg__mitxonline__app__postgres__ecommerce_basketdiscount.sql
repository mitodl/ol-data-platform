with source as (

    select *
    from
        {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_basketdiscount') }}

)

, renamed as (

    select
        id as basketdiscount_id
        , redeemed_by_id as user_id
        , redeemed_basket_id as basket_id
        , redeemed_discount_id as discount_id
        , to_iso8601(
            from_iso8601_timestamp(redemption_date)
        ) as basketdiscount_applied_on
        , to_iso8601(
            from_iso8601_timestamp(created_on)
        ) as basketdiscount_created_on
        , to_iso8601(
            from_iso8601_timestamp(updated_on)
        ) as basketdiscount_updated_on

    from source

)

select * from renamed

with source as (

    select *
    from
        {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_order') }}

)

, renamed as (
    select
        id as order_id
        , status as order_state
        , total_price_paid as order_total_price_paid
        , purchaser_id as order_purchaser_user_id
        , to_iso8601(from_iso8601_timestamp(updated_on)) as order_updated_on
        , to_iso8601(from_iso8601_timestamp(created_on)) as order_created_on
    from source
)

select * from renamed

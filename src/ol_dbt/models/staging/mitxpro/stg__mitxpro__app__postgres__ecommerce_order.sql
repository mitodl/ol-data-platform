with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_order') }}

)

, renamed as (
    select
        id as order_id
        , status as order_state
        , total_price_paid as order_total_price_paid
        , purchaser_id as order_purchaser_user_id
        , {{ cast_timestamp_to_iso8601('updated_on') }} as order_updated_on
        , {{ cast_timestamp_to_iso8601('created_on') }} as order_created_on
    from source
)

select * from renamed

with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_order') }}

)

, renamed as (

    select
        id as order_id
        , state as order_state
        , purchaser_id as order_purchaser_user_id
        , reference_number as order_reference_number
        , total_price_paid as order_total_price_paid
        , {{ cast_timestamp_to_iso8601('created_on') }} as order_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as order_updated_on
    from source

)

select * from renamed

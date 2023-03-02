with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__ecommerce_receipt') }}

)

, renamed as (
    select
        id as receipt_id
        , order_id
        , data as receipt_data
        , {{ cast_timestamp_to_iso8601('created_at') }} as receipt_created_on
        , {{ cast_timestamp_to_iso8601('modified_at') }} as receipt_updated_on
    from source

)

select * from renamed

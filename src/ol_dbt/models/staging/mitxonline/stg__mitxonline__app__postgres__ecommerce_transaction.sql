with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_transaction') }}

)

, renamed as (

    select
        id as transaction_id
        , data as transaction_data
        , amount as transaction_amount
        , order_id
        , transaction_id as transaction_readable_identifier
        , transaction_type as transaction_type
        ,{{ cast_timestamp_to_iso8601('created_on') }} as transaction_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as transaction_updated_on
    from source

)

select * from renamed

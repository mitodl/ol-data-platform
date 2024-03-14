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
        , transaction_type
        , json_query(data, 'lax $.decision' omit quotes) as transaction_status
        , json_query(data, 'lax $.auth_code' omit quotes) as transaction_authorization_code
        , json_query(data, 'lax $.req_payment_method' omit quotes) as transaction_payment_method
        , json_query(data, 'lax $.req_reference_number' omit quotes) as transaction_reference_number
        , json_query(data, 'lax $.req_bill_to_address_state' omit quotes) as transaction_bill_to_address_state
        , json_query(data, 'lax $.req_bill_to_address_country' omit quotes) as transaction_bill_to_address_country
        ,{{ cast_timestamp_to_iso8601('created_on') }} as transaction_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as transaction_updated_on
    from source

)

select * from renamed

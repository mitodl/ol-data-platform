with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__ecommerce_receipt') }}

)

, renamed as (
    select
        id as receipt_id
        , order_id
        , json_query(data, 'lax $.decision' omit quotes) as receipt_transaction_status
        , json_query(data, 'lax $.transaction_id' omit quotes) as receipt_transaction_id
        , json_query(data, 'lax $.req_payment_method' omit quotes) as receipt_payment_method
        , json_query(data, 'lax $.auth_code' omit quotes) as receipt_authorization_code
        , json_query(data, 'lax $.req_reference_number' omit quotes) as receipt_reference_number
        , json_query(data, 'lax $.req_bill_to_address_state' omit quotes) as receipt_bill_to_address_state
        , json_query(data, 'lax $.req_bill_to_address_country' omit quotes) as receipt_bill_to_address_country
        , data as receipt_data
        ,{{ cast_timestamp_to_iso8601('created_at') }} as receipt_created_on
        ,{{ cast_timestamp_to_iso8601('modified_at') }} as receipt_updated_on
    from source

)

select * from renamed

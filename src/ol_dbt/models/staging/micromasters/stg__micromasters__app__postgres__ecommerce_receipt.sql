with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__ecommerce_receipt') }}

)

, renamed as (
    select
        id as receipt_id
        , order_id
        , json_query(data, 'lax $.decision' omit quotes) as receipt_transaction_status
        , json_query(data, 'lax $.transaction_id' omit quotes) as receipt_transaction_id
        , json_query(data, 'lax $.req_transaction_uuid' omit quotes) as receipt_transaction_uuid
        , json_query(data, 'lax $.req_transaction_type' omit quotes) as receipt_transaction_type
        , json_query(data, 'lax $.req_payment_method' omit quotes) as receipt_payment_method
        , json_query(data, 'lax $.auth_code' omit quotes) as receipt_authorization_code
        , json_query(data, 'lax $.req_reference_number' omit quotes) as receipt_reference_number
        , json_query(data, 'lax $.req_bill_to_address_state' omit quotes) as receipt_bill_to_address_state
        , json_query(data, 'lax $.req_bill_to_address_country' omit quotes) as receipt_bill_to_address_country
        , cast(json_query(data, 'lax $.req_amount' omit quotes) as decimal(38, 2)) as receipt_payment_amount
        , json_query(data, 'lax $.req_currency' omit quotes) as receipt_payment_currency
        , json_query(data, 'lax $.req_bill_to_email' omit quotes) as receipt_payer_email
        , json_query(data, 'lax $.req_card_number' omit quotes) as receipt_payment_card_number
        , json_query(data, 'lax $.req_customer_ip_address' omit quotes) as receipt_payer_ip_address
        , json_query(data, 'lax $.card_type_name' omit quotes) as receipt_payment_card_type
        , concat(
            json_query(data, 'lax $.req_bill_to_forename' omit quotes)
            , ' '
            , json_query(data, 'lax $.req_bill_to_surname' omit quotes)
        ) as receipt_payer_name
        , data as receipt_data
        , json_query(data, 'lax $.signed_date_time' omit quotes) as receipt_payment_timestamp
        ,{{ cast_timestamp_to_iso8601('created_at') }} as receipt_created_on
        ,{{ cast_timestamp_to_iso8601('modified_at') }} as receipt_updated_on
    from source

)

select * from renamed

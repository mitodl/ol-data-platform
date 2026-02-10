with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__ecommerce_receipt') }}

)

, renamed as (
    select
        id as receipt_id
        , order_id
        , {{ json_query_string('data', "'$.decision'") }} as receipt_transaction_status
        , {{ json_query_string('data', "'$.transaction_id'") }} as receipt_transaction_id
        , {{ json_query_string('data', "'$.req_transaction_uuid'") }} as receipt_transaction_uuid
        , {{ json_query_string('data', "'$.req_transaction_type'") }} as receipt_transaction_type
        , {{ json_query_string('data', "'$.req_payment_method'") }} as receipt_payment_method
        , {{ json_query_string('data', "'$.auth_code'") }} as receipt_authorization_code
        , {{ json_query_string('data', "'$.req_reference_number'") }} as receipt_reference_number
        , {{ json_query_string('data', "'$.req_bill_to_address_state'") }} as receipt_bill_to_address_state
        , {{ json_query_string('data', "'$.req_bill_to_address_country'") }} as receipt_bill_to_address_country
        , cast({{ json_query_string('data', "'$.req_amount'") }} as decimal(38, 2)) as receipt_payment_amount
        , {{ json_query_string('data', "'$.req_currency'") }} as receipt_payment_currency
        , {{ json_query_string('data', "'$.req_bill_to_email'") }} as receipt_payer_email
        , {{ json_query_string('data', "'$.req_card_number'") }} as receipt_payment_card_number
        , {{ json_query_string('data', "'$.req_customer_ip_address'") }} as receipt_payer_ip_address
        , {{ json_query_string('data', "'$.card_type_name'") }} as receipt_payment_card_type
        , concat(
            {{ json_query_string('data', "'$.req_bill_to_forename'") }}
            , ' '
            , {{ json_query_string('data', "'$.req_bill_to_surname'") }}
        ) as receipt_payer_name
        , data as receipt_data
        , {{ json_query_string('data', "'$.signed_date_time'") }} as receipt_payment_timestamp
        ,{{ cast_timestamp_to_iso8601('created_at') }} as receipt_created_on
        ,{{ cast_timestamp_to_iso8601('modified_at') }} as receipt_updated_on
    from source

)

select * from renamed

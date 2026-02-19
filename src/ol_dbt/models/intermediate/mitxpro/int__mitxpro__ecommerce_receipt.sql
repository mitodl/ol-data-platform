with receipts as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_receipt') }}
)

select
    receipt_id
    , receipt_created_on
    , receipt_updated_on
    , receipt_data
    , order_id
    , cast({{ json_query_string('receipt_data', "'$.req_amount'") }} as decimal(38, 2)) as receipt_payment_amount
    , {{ json_query_string('receipt_data', "'$.req_transaction_uuid'") }} as receipt_transaction_uuid
    , {{ json_query_string('receipt_data', "'$.decision'") }} as receipt_transaction_status
    , {{ json_query_string('receipt_data', "'$.transaction_id'") }} as receipt_transaction_id
    , {{ json_query_string('receipt_data', "'$.auth_code'") }} as receipt_authorization_code
    , {{ json_query_string('receipt_data', "'$.req_payment_method'") }} as receipt_payment_method
    , {{ json_query_string('receipt_data', "'$.req_reference_number'") }} as receipt_reference_number
    , {{ json_query_string('receipt_data', "'$.req_bill_to_address_state'") }} as receipt_bill_to_address_state
    , {{ json_query_string('receipt_data', "'$.req_bill_to_address_country'") }} as receipt_bill_to_address_country
    , {{ json_query_string('receipt_data', "'$.req_transaction_type'") }} as receipt_transaction_type
    , {{ json_query_string('receipt_data', "'$.req_currency'") }} as receipt_payment_currency
    , {{ json_query_string('receipt_data', "'$.req_bill_to_email'") }} as receipt_payer_email
    , {{ json_query_string('receipt_data', "'$.req_card_number'") }} as receipt_payment_card_number
    , {{ json_query_string('receipt_data', "'$.req_customer_ip_address'") }} as receipt_payer_ip_address
    , {{ json_query_string('receipt_data', "'$.card_type_name'") }} as receipt_payment_card_type
    , concat(
        {{ json_query_string('receipt_data', "'$.req_bill_to_forename'") }}
        , ' '
        , {{ json_query_string('receipt_data', "'$.req_bill_to_surname'") }}
    ) as receipt_payer_name
    , {{ json_query_string('receipt_data', "'$.signed_date_time'") }} as receipt_payment_timestamp
from receipts

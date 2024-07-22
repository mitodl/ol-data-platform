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
    , json_query(data, 'lax $.req_transaction_uuid' omit quotes) as receipt_transaction_uuid
    , json_query(data, 'lax $.decision' omit quotes) as receipt_transaction_status
    , json_query(data, 'lax $.transaction_id' omit quotes) as receipt_transaction_id
    , json_query(data, 'lax $.auth_code' omit quotes) as receipt_authorization_code
    , json_query(data, 'lax $.req_payment_method' omit quotes) as receipt_payment_method
    , json_query(data, 'lax $.req_reference_number' omit quotes) as receipt_reference_number
    , json_query(data, 'lax $.req_bill_to_address_state' omit quotes) as receipt_bill_to_address_state
    , json_query(data, 'lax $.req_bill_to_address_country' omit quotes) as receipt_bill_to_address_country
    , json_query(data, 'lax $.req_transaction_type' omit quotes) as receipt_transaction_type
    , json_query(data, 'lax $.req_amount' omit quotes) as receipt_payment_amount
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
    , json_query(data, 'lax $.signed_date_time' omit quotes) as receipt_payment_timestamp
from receipts

with transactions as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_transaction') }}
)

select
    transaction_id
    , transaction_data
    , transaction_amount
    , order_id
    , transaction_created_on
    , transaction_readable_identifier
    , transaction_type
    , json_query(transaction_data, 'lax $.decision' omit quotes) as transaction_status
    , json_query(transaction_data, 'lax $.req_payment_method' omit quotes) as transaction_payment_method
    , json_query(transaction_data, 'lax $.req_reference_number' omit quotes) as transaction_reference_number
    , json_query(transaction_data, 'lax $.req_bill_to_address_state' omit quotes) as transaction_bill_to_address_state
    , json_query(
        transaction_data, 'lax $.req_bill_to_address_country' omit quotes
    ) as transaction_bill_to_address_country
    , json_query(transaction_data, 'lax $.req_transaction_type' omit quotes) as transaction_payment_type
    , json_query(transaction_data, 'lax $.req_amount' omit quotes) as transaction_payment_amount
    , json_query(transaction_data, 'lax $.req_bill_to_email' omit quotes) as transaction_payer_email
    , json_query(transaction_data, 'lax $.req_card_number' omit quotes) as transaction_payment_card_number
    , json_query(transaction_data, 'lax $.req_customer_ip_address' omit quotes) as transaction_payer_ip_address
    , json_query(transaction_data, 'lax $.card_type_name' omit quotes) as transaction_payment_card_type
    , concat(
        json_query(transaction_data, 'lax $.req_bill_to_forename' omit quotes)
        , ' '
        , json_query(transaction_data, 'lax $.req_bill_to_surname' omit quotes)
    ) as transaction_payer_name
    , case
        when transaction_type = 'payment'
            then json_query(transaction_data, 'lax $.req_transaction_uuid' omit quotes)
        when transaction_type = 'refund'
            then json_query(transaction_data, 'lax $.clientReferenceInformation.transactionId' omit quotes)
    end as transaction_id_from_cybersource
    , case
        when transaction_type = 'payment'
            then json_query(transaction_data, 'lax $.auth_code' omit quotes)
        when transaction_type = 'refund'
            then json_query(transaction_data, 'lax $.processorInformation.approvalCode' omit quotes)
    end as transaction_authorization_code
    , case
        when transaction_type = 'payment'
            then json_query(transaction_data, 'lax $.signed_date_time' omit quotes)
        when transaction_type = 'refund'
            then json_query(transaction_data, 'lax $.submitTimeUtc' omit quotes)
    end as transaction_timestamp
from transactions

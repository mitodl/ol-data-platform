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
    , transaction_status
    , transaction_authorization_code
    , transaction_payment_method
    , transaction_reference_number
    , transaction_bill_to_address_state
    , transaction_bill_to_address_country
    , json_query(transaction_data, 'lax $.req_transaction_type' omit quotes) as transaction_payment_type
    , concat(
        json_query(transaction_data, 'lax $.req_bill_to_forename' omit quotes)
        , ' '
        , json_query(transaction_data, 'lax $.req_bill_to_surname' omit quotes)
    ) as transaction_payer
    , case
        when transaction_type = 'payment'
            then json_query(transaction_data, 'lax $.req_transaction_uuid' omit quotes)
        when transaction_type = 'refund'
            then json_query(transaction_data, 'lax $.clientReferenceInformation.transactionId' omit quotes)
    end as transaction_uuid
    , case
        when transaction_type = 'payment'
            then json_query(transaction_data, 'lax $.auth_code' omit quotes)
        when transaction_type = 'refund'
            then json_query(transaction_data, 'lax $.processorInformation.approvalCode' omit quotes)
    end as transaction_auth_approval_code
    , case
        when transaction_type = 'payment'
            then json_query(transaction_data, 'lax $.signed_date_time' omit quotes)
        when transaction_type = 'refund'
            then json_query(transaction_data, 'lax $.submitTimeUtc' omit quotes)
    end as transaction_timestamp
    , case
        when transaction_type = 'payment'
            then json_query(transaction_data, 'lax $.req_amount' omit quotes)
        when transaction_type = 'refund'
            then json_query(transaction_data, 'lax $.refundAmountDetails.refundAmount' omit quotes)
    end as transaction_req_refund_amount
from transactions

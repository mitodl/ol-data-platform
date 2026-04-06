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
    , cast({{ json_query_string('transaction_data', "'$.req_amount'") }} as decimal(38, 2)) as transaction_payment_amount
    , {{ json_query_string('transaction_data', "'$.req_transaction_uuid'") }} as transaction_uuid
    , {{ json_query_string('transaction_data', "'$.decision'") }} as transaction_status
    , case {{ json_query_string('transaction_data', "'$.req_payment_method'") }}
        when 'card' then 'credit_card'
        else {{ json_query_string('transaction_data', "'$.req_payment_method'") }}
    end as transaction_payment_method
    , {{ json_query_string('transaction_data', "'$.req_reference_number'") }} as transaction_reference_number
    , {{ json_query_string('transaction_data', "'$.req_bill_to_address_state'") }} as transaction_bill_to_address_state
    , {{ json_query_string('transaction_data', "'$.req_bill_to_address_country'") }}
        as transaction_bill_to_address_country
    , {{ json_query_string('transaction_data', "'$.req_transaction_type'") }} as transaction_req_type
    , {{ json_query_string('transaction_data', "'$.req_currency'") }} as transaction_payment_currency
    , {{ json_query_string('transaction_data', "'$.req_bill_to_email'") }} as transaction_payer_email
    , {{ json_query_string('transaction_data', "'$.req_card_number'") }} as transaction_payment_card_number
    , {{ json_query_string('transaction_data', "'$.req_customer_ip_address'") }} as transaction_payer_ip_address
    , {{ json_query_string('transaction_data', "'$.card_type_name'") }} as transaction_payment_card_type
    , concat(
        {{ json_query_string('transaction_data', "'$.req_bill_to_forename'") }}
        , ' '
        , {{ json_query_string('transaction_data', "'$.req_bill_to_surname'") }}
    ) as transaction_payer_name
    , coalesce(
        {{ json_query_string('transaction_data', "'$.auth_code'") }}
        , {{ json_query_string('transaction_data', "'$.processorInformation.approvalCode'") }}
    ) as transaction_authorization_code
    , coalesce(
        {{ json_query_string('transaction_data', "'$.signed_date_time'") }}
        , {{ json_query_string('transaction_data', "'$.submitTimeUtc'") }}
    ) as transaction_timestamp
from transactions

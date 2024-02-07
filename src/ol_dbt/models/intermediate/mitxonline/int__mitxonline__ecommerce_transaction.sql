with transactions as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_transaction') }}
)

select
    transactions.transaction_id
    , transaction_data
    , transactions.transaction_amount
    , transactions.order_id
    , transactions.transaction_created_on
    , transactions.transaction_readable_identifier
    , transactions.transaction_type
    , transaction_status
    , transaction_authorization_code
    , transaction_payment_method
    , transaction_reference_number
    , transaction_bill_to_address_state
    , transaction_bill_to_address_country
from transactions

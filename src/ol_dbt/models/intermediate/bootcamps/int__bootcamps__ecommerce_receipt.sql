with receipts as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__ecommerce_receipt') }}
)

select
    receipt_id
    , receipt_created_on
    , receipt_updated_on
    , receipt_transaction_status
    , receipt_transaction_id
    , receipt_payment_method
    , receipt_authorization_code
    , receipt_reference_number
    , receipt_bill_to_address_state
    , receipt_bill_to_address_country
    , receipt_data
    , order_id
from receipts

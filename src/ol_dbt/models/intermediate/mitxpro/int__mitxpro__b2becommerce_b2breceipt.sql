with b2breceipts as (select * from {{ ref("stg__mitxpro__app__postgres__b2becommerce_b2breceipt") }})

select
    b2breceipt_id,
    b2breceipt_created_on,
    b2breceipt_updated_on,
    b2breceipt_data,
    b2border_id,
    b2breceipt_transaction_status,
    b2breceipt_transaction_id,
    b2breceipt_authorization_code,
    b2breceipt_reference_number,
    b2breceipt_payment_method,
    b2breceipt_bill_to_address_state,
    b2breceipt_bill_to_address_country
from b2breceipts

with receipts as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_reciept') }}
)



select
    receipt_id
    , receipt_created_on
    , receipt_updated_on
    , receipt_data
    , order_id
from receipts

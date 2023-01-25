with receipts as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__ecommerce_receipt') }}
)

select
    receipt_id
    , receipt_created_on
    , receipt_updated_on
    , receipt_data
    , order_id
from receipts

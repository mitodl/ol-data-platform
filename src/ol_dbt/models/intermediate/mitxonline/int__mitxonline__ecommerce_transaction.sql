with transactions as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_transaction') }}
)

select
    transactions.transaction_id
    , transactions.transaction_amount
    , transactions.order_id
    , transactions.transaction_created_on
    , transactions.transaction_readable_identifier
    , transactions.transaction_type
from transactions

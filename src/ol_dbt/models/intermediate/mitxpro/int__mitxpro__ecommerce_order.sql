with orders as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_order') }}
)



select
    order_id
    , order_state
    , order_purchaser_user_id
    , order_total_price_paid
    , order_created_on
    , order_updated_on
from orders

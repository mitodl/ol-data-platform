with orders as (
    select * from {{ ref('marts__combined__orders') }}
)

, products as (
    select * from {{ ref('marts__combined__products') }}
)

select
    orders.*
    , products.product_name
from orders
left join products
    on orders.product_readable_id = products.product_readable_id

-- tfact_order and marts__combined__orders subtract an xPRO order's total from each line's
-- price, which is only right while every multi-line order has a total of 0. A row here
-- means a line's discount_amount is wrong, not that the order is, so this warns.
{{ config(severity='warn') }}

select orders.order_id
from {{ ref('int__mitxpro__ecommerce_order') }} as orders
inner join {{ ref('int__mitxpro__ecommerce_line') }} as lines
    on orders.order_id = lines.order_id
where orders.order_total_price_paid != 0
group by orders.order_id
having count(distinct lines.line_id) > 1

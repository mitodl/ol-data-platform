with orders as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_order') }}
)

, users as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

select
    orders.order_id
    , orders.order_state
    , orders.order_created_on
    , orders.order_purchaser_user_id
    , orders.order_reference_number
    , orders.order_total_price_paid
    , users.user_username as order_purchaser_username
    , users.user_full_name as order_purchaser_full_name
    , users.user_email as order_purchaser_email
from orders
inner join users on users.user_id = orders.order_purchaser_user_id

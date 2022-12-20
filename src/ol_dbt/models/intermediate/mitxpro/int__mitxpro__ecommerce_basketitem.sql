with basketitem as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_basketitem') }}
)

, basket as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_basket') }}
)

select
    basketitem.basketitem_id
    , basketitem.basketitem_quantity
    , basketitem.basket_id
    , basketitem.basketitem_created_on
    , basketitem.product_id
    , basketitem.basketitem_updated_on
    , basketitem.programrun_id
    , basket.user_id
from basketitem
inner join basket on basketitem.basket_id = basket.basket_id

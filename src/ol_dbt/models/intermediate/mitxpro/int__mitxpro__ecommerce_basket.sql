with basket as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_basket') }}
)

, couponbasket as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponbasket') }}
)

select
    basket.basket_id
    , basket.user_id
    , basket.basket_created_on
    , basket.basket_updated_on
    , couponbasket.coupon_id
from basket
left join couponbasket on basket.basket_id = couponbasket.basket_id

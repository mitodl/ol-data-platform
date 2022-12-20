with basketrunselection as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_basketrunselection') }}
)

, basket as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_basket') }}
)

select
    basketrunselection.basketrunselection_id
    , basketrunselection.basket_id
    , basketrunselection.courserun_id
    , basketrunselection.basketrunselection_created_on
    , basketrunselection.basketrunselection_updated_on
    , basket.user_id
from basketrunselection
inner join basket on basketrunselection.basket_id = basket.basket_id

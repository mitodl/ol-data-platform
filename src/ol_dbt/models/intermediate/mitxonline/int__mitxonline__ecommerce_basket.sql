with source as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_basket') }}
)

select
    basket_id
    , user_id
    , basket_created_on
    , basket_updated_on
from source

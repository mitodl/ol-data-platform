with source as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_basketdiscount') }}
)

select
    basketdiscount_id
    , basketdiscount_created_on
    , basketdiscount_updated_on
    , user_id
    , basketdiscount_applied_on
    , basket_id
    , discount_id
from source

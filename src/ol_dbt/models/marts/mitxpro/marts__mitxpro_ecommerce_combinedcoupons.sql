with ecommerce_combinedcoupons as (
    select *
    from {{ ref('int__mitxpro__ecommerce_combinedcoupons') }}
)

select *
from ecommerce_combinedcoupons

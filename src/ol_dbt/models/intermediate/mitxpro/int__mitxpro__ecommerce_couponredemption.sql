with ecommerce_couponredemption as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponredemption') }}
)


select
    couponredemption_id
    , order_id
    , couponversion_id
    , couponredemption_created_on
    , couponredemption_updated_on
from ecommerce_couponredemption

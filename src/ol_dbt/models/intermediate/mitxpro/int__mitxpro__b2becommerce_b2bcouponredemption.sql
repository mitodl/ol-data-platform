with b2bcouponredemption as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__b2becommerce_b2bcouponredemption') }}
)

select
    b2bcouponredemption_id
    , b2border_id
    , b2bcoupon_id
    , b2bcouponredemption_updated_on
    , b2bcouponredemption_created_on
from b2bcouponredemption

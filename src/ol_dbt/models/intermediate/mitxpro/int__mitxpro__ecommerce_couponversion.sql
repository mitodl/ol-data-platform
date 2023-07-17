with ecommerce_couponversion as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponversion') }}
)


select
    couponversion_id
    , coupon_id
    , couponpaymentversion_id
    , couponversion_updated_on
    , couponversion_created_on
from ecommerce_couponversion

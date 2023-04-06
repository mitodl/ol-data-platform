with b2bcoupon as (
    select *
    from dev.main_staging.stg__mitxpro__app__postgres__b2becommerce_b2bcoupon
)

select
    b2bcoupon_id
    , b2bcoupon_updated_on
    , b2bcoupon_created_on
    , b2bcoupon_expires_on
    , company_id
    , product_id
    , b2bcoupon_is_enabled
    , b2bcoupon_activated_on
    , b2bcoupon_discount_percent
    , b2bcoupon_name
    , b2bcoupon_is_reusable
    , b2bcoupon_coupon_code
from b2bcoupon

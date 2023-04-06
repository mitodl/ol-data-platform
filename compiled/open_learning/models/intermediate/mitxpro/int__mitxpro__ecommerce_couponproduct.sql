with couponproduct as (
    select *
    from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_couponproduct
)

select
    couponproduct_id
    , product_id
    , coupon_id
    , couponproduct_created_on
    , couponproduct_updated_on
    , programrun_id
from couponproduct

with
    ecommerce_couponversion as (select * from {{ ref("stg__mitxpro__app__postgres__ecommerce_couponversion") }}),
    latest_ecommerce_couponversion as (
        select coupon_id, max(couponversion_updated_on) as max_couponversion_updated_on
        from ecommerce_couponversion
        group by coupon_id
    )

select
    ecommerce_couponversion.couponversion_id,
    ecommerce_couponversion.coupon_id,
    ecommerce_couponversion.couponpaymentversion_id,
    ecommerce_couponversion.couponversion_updated_on,
    ecommerce_couponversion.couponversion_created_on,
    case
        when latest_ecommerce_couponversion.max_couponversion_updated_on is not null then 'Y' else 'N'
    end as is_latest_couponversion
from ecommerce_couponversion
left join
    latest_ecommerce_couponversion
    on ecommerce_couponversion.coupon_id = latest_ecommerce_couponversion.coupon_id
    and ecommerce_couponversion.couponversion_updated_on = latest_ecommerce_couponversion.max_couponversion_updated_on

with ecommerce_couponpayment as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_couponpayment') }}
)


select
    couponpayment_id
    , couponpayment_name
    , couponpayment_updated_on
    , couponpayment_created_on
from ecommerce_couponpayment

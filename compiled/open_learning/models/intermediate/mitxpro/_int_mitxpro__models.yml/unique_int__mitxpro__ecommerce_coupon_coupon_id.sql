select
    coupon_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__ecommerce_coupon
where coupon_id is not null
group by coupon_id
having count(*) > 1

select
    coupon_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__micromasters__app__postgres__ecommerce_coupon
where coupon_id is not null
group by coupon_id
having count(*) > 1

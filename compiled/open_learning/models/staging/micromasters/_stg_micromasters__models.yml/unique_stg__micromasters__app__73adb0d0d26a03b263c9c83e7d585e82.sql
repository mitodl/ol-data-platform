select
    usercoupon_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__micromasters__app__postgres__ecommerce_usercoupon
where usercoupon_id is not null
group by usercoupon_id
having count(*) > 1

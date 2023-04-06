select
    b2bcoupon_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__b2becommerce_b2bcoupon
where b2bcoupon_id is not null
group by b2bcoupon_id
having count(*) > 1

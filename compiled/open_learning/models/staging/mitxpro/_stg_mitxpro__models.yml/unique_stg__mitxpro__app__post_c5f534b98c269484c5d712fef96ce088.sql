select
    productcouponassignment_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_productcouponassignment
where productcouponassignment_id is not null
group by productcouponassignment_id
having count(*) > 1

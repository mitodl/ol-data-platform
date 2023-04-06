select
    bulkcouponassignment_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_bulkcouponassignment
where bulkcouponassignment_id is not null
group by bulkcouponassignment_id
having count(*) > 1

select
    couponversion_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_couponversion
where couponversion_id is not null
group by couponversion_id
having count(*) > 1

select
    couponpayment_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_couponpayment
where couponpayment_id is not null
group by couponpayment_id
having count(*) > 1

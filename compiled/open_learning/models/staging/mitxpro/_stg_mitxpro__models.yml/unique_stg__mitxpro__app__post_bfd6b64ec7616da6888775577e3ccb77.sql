select
    couponpayment_name as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_couponpayment
where couponpayment_name is not null
group by couponpayment_name
having count(*) > 1

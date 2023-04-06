select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select couponpaymentversion_coupon_type
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_couponpaymentversion
    where couponpaymentversion_coupon_type is null




) as dbt_internal_test

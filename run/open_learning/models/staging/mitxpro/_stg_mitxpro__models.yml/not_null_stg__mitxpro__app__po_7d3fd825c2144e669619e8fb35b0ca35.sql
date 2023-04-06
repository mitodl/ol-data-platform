select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select coupon_is_global
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_coupon
    where coupon_is_global is null




) as dbt_internal_test

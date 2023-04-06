select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select b2bcoupon_discount_percent
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__b2becommerce_b2bcoupon
    where b2bcoupon_discount_percent is null




) as dbt_internal_test

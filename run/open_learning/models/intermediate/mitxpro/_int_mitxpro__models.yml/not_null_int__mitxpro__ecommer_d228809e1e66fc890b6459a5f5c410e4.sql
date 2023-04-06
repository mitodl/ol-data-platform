select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select productcouponassignment_updated_on
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_productcouponassignment
    where productcouponassignment_updated_on is null




) as dbt_internal_test

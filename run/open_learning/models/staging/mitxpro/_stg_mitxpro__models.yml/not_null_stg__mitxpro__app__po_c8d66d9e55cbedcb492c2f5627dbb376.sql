select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select productcouponassignment_email
    from
        ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_productcouponassignment
    where productcouponassignment_email is null




) as dbt_internal_test

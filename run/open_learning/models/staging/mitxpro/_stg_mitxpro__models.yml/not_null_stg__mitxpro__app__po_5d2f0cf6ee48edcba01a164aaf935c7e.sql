select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select product_created_on
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_product
    where product_created_on is null




) as dbt_internal_test

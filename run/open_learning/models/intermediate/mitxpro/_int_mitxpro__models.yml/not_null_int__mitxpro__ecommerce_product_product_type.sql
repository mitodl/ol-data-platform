select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select product_type
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_product
    where product_type is null




) as dbt_internal_test

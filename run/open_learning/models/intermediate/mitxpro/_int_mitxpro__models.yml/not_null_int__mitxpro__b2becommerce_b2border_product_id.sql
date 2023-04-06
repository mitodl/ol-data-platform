select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select product_id
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__b2becommerce_b2border
    where product_id is null




) as dbt_internal_test

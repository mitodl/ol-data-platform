select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select receipt_data
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_receipt
    where receipt_data is null




) as dbt_internal_test

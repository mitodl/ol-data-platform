select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select b2breceipt_data
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__b2becommerce_b2breceipt
    where b2breceipt_data is null




) as dbt_internal_test

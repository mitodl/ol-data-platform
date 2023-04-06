select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select order_reference_number
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_order
    where order_reference_number is null




) as dbt_internal_test

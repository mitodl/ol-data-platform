select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select productversion_requires_enrollment_code
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_productversion
    where productversion_requires_enrollment_code is null




) as dbt_internal_test

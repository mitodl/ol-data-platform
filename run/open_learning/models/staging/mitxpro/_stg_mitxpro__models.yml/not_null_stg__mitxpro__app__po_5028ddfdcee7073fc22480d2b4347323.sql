select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select productversion_id
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__b2becommerce_b2border
    where productversion_id is null




) as dbt_internal_test

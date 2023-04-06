select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select electiveset_required_number
    from ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__courses_electiveset
    where electiveset_required_number is null




) as dbt_internal_test

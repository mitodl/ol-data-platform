select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select program_is_live
    from ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__courses_program
    where program_is_live is null




) as dbt_internal_test

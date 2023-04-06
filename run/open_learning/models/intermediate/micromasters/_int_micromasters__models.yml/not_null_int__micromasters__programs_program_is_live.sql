select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select program_is_live
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__micromasters__programs
    where program_is_live is null




) as dbt_internal_test

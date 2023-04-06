select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select programrun_readable_id
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__program_runs
    where programrun_readable_id is null




) as dbt_internal_test

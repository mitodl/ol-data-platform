select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select courserun_tag
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__course_runs
    where courserun_tag is null




) as dbt_internal_test

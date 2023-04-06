select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select courserun_title
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__courserun_grades
    where courserun_title is null




) as dbt_internal_test

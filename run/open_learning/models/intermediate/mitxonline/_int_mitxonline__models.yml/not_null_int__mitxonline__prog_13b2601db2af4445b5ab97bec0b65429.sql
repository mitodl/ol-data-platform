select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select programrequirement_path
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__programrequirements
    where programrequirement_path is null




) as dbt_internal_test

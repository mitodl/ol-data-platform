select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select programrequirement_depth
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__programrequirements
    where programrequirement_depth is null




) as dbt_internal_test

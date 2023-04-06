select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select courseware_object_id
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_flexiblepricetier
    where courseware_object_id is null




) as dbt_internal_test

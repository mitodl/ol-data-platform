select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select program_title
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__program_certificates
    where program_title is null




) as dbt_internal_test

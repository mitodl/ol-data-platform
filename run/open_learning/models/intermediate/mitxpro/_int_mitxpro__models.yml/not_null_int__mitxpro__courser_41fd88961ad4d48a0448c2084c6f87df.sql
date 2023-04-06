select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select courseruncertificate_uuid
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__courserun_certificates
    where courseruncertificate_uuid is null




) as dbt_internal_test

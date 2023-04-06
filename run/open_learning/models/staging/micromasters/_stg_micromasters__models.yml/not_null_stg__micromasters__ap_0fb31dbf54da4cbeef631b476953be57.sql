select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select programcertificate_hash
    from
        ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__grades_programcertificate
    where programcertificate_hash is null




) as dbt_internal_test

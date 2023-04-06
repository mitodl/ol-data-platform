select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select revision_date_created
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__reversion_revision
    where revision_date_created is null




) as dbt_internal_test

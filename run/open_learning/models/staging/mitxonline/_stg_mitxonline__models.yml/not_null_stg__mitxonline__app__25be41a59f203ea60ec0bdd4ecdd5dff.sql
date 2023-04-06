select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select flexiblepriceapplication_status
    from
        ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepriceapplication
    where flexiblepriceapplication_status is null




) as dbt_internal_test

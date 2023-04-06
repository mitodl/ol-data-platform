select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select flexiblepricetier_is_current
    from
        ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepricetier
    where flexiblepricetier_is_current is null




) as dbt_internal_test

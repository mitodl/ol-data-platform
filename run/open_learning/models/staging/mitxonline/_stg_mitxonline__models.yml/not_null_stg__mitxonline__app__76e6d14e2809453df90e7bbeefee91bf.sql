select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select countryincomethreshold_income_threshold
    from
        ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_countryincomethreshold
    where countryincomethreshold_income_threshold is null




) as dbt_internal_test

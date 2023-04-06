select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select countryincomethreshold_created_on
    from
        ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_countryincomethreshold
    where countryincomethreshold_created_on is null




) as dbt_internal_test

select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select currencyexchangerate_exchange_rate
    from
        ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_currencyexchangerate
    where currencyexchangerate_exchange_rate is null




) as dbt_internal_test

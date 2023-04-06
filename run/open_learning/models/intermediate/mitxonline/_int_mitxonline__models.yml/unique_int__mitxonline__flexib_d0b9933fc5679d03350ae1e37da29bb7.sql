select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        currencyexchangerate_currency_code as unique_field
        , count(*) as n_records

    from
        ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_currencyexchangerate
    where currencyexchangerate_currency_code is not null
    group by currencyexchangerate_currency_code
    having count(*) > 1




) as dbt_internal_test

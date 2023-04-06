select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        countryincomethreshold_country_code as unique_field
        , count(*) as n_records

    from
        ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_countryincomethreshold
    where countryincomethreshold_country_code is not null
    group by countryincomethreshold_country_code
    having count(*) > 1




) as dbt_internal_test

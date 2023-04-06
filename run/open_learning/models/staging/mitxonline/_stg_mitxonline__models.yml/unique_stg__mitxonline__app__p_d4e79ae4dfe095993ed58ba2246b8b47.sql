select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        flexiblepricetier_id as unique_field
        , count(*) as n_records

    from
        ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepricetier
    where flexiblepricetier_id is not null
    group by flexiblepricetier_id
    having count(*) > 1




) as dbt_internal_test

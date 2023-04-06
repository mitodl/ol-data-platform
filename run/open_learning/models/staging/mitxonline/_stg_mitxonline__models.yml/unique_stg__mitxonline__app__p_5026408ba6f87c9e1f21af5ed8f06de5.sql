select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        coursetopic_name as unique_field
        , count(*) as n_records

    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_coursetopic
    where coursetopic_name is not null
    group by coursetopic_name
    having count(*) > 1




) as dbt_internal_test

select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        programenrollment_id as unique_field
        , count(*) as n_records

    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_programenrollment
    where programenrollment_id is not null
    group by programenrollment_id
    having count(*) > 1




) as dbt_internal_test

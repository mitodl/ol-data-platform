select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        contenttype_id as unique_field
        , count(*) as n_records

    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__django_contenttype
    where contenttype_id is not null
    group by contenttype_id
    having count(*) > 1




) as dbt_internal_test

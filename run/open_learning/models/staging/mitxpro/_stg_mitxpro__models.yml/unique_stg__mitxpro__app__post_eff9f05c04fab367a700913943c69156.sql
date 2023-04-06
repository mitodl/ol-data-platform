select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        contenttype_full_name as unique_field
        , count(*) as n_records

    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__django_contenttype
    where contenttype_full_name is not null
    group by contenttype_full_name
    having count(*) > 1




) as dbt_internal_test

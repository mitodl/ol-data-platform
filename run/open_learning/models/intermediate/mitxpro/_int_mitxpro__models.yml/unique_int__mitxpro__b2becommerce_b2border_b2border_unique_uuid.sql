select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        b2border_unique_uuid as unique_field
        , count(*) as n_records

    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__b2becommerce_b2border
    where b2border_unique_uuid is not null
    group by b2border_unique_uuid
    having count(*) > 1




) as dbt_internal_test

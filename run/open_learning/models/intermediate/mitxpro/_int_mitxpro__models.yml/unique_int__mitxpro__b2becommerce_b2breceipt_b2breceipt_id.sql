select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        b2breceipt_id as unique_field
        , count(*) as n_records

    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__b2becommerce_b2breceipt
    where b2breceipt_id is not null
    group by b2breceipt_id
    having count(*) > 1




) as dbt_internal_test

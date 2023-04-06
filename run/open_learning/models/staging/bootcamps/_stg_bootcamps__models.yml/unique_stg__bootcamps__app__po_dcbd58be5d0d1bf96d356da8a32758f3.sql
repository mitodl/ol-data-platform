select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        receipt_id as unique_field
        , count(*) as n_records

    from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__ecommerce_receipt
    where receipt_id is not null
    group by receipt_id
    having count(*) > 1




) as dbt_internal_test

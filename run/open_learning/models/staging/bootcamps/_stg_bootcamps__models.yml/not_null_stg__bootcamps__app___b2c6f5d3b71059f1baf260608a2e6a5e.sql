select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select wiretransferreceipt_data
    from
        ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__ecommerce_wiretransferreceipt
    where wiretransferreceipt_data is null




) as dbt_internal_test

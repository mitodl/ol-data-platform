select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select product_version_id
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_line
    where product_version_id is null




) as dbt_internal_test

select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select order_purchaser_user_id
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_order
    where order_purchaser_user_id is null




) as dbt_internal_test

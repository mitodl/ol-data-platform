select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select user_full_name
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__users
    where user_full_name is null




) as dbt_internal_test

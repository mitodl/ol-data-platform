select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select user_full_name
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__users_user
    where user_full_name is null




) as dbt_internal_test

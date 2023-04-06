select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select user_address_id
    from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__users_legaladdress
    where user_address_id is null




) as dbt_internal_test

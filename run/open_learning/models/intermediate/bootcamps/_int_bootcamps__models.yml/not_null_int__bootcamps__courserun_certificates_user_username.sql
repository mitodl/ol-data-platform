select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select user_username
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__bootcamps__courserun_certificates
    where user_username is null




) as dbt_internal_test

select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select user_email
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__combined__courserun_enrollments
    where user_email is null




) as dbt_internal_test

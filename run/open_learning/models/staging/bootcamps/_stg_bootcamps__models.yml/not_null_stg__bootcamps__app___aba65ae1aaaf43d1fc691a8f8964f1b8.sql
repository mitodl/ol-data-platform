select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select user_job_title
    from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__profiles_profile
    where user_job_title is null




) as dbt_internal_test

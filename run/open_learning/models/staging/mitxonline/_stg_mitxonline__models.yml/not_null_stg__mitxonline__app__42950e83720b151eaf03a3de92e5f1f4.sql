select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select courserunenrollment_updated_on
    from
        ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_courserunenrollment
    where courserunenrollment_updated_on is null




) as dbt_internal_test

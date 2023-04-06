select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select courserunenrollment_is_active
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__courserunenrollments
    where courserunenrollment_is_active is null




) as dbt_internal_test

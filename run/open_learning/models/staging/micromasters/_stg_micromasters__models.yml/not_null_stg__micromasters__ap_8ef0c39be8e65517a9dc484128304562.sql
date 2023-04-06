select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (






    select course_position_in_program
    from ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__courses_course
    where course_position_in_program is null




) as dbt_internal_test

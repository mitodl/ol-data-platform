select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            program_id
            , course_position_in_program
        from ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__courses_course
        where
            1 = 1
            and
            not (
                program_id is null
                and course_position_in_program is null

            )



        group by
            program_id, course_position_in_program
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

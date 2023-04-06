select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            course_id
            , electiveset_id
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__courses_electiveset_to_course
        where
            1 = 1
            and
            not (
                course_id is null
                and electiveset_id is null

            )



        group by
            course_id, electiveset_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

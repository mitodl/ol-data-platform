select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            coursetopic_id
            , course_id
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_course_to_topic
        where
            1 = 1
            and
            not (
                coursetopic_id is null
                and course_id is null

            )



        group by
            coursetopic_id, course_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

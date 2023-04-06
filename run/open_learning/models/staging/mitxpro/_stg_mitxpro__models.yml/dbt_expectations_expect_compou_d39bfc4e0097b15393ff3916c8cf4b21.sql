select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            course_id
            , courserun_tag
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_courserun
        where
            1 = 1
            and
            not (
                course_id is null
                and courserun_tag is null

            )



        group by
            course_id, courserun_tag
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            courserun_id
            , user_id
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_courserunenrollment
        where
            1 = 1
            and
            not (
                courserun_id is null
                and user_id is null

            )



        group by
            courserun_id, user_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

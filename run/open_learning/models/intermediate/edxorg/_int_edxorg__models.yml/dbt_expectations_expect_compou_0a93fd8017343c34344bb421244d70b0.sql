select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            user_id
            , courserun_readable_id
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__edxorg__mitx_courserun_grades
        where
            1 = 1
            and
            not (
                user_id is null
                and courserun_readable_id is null

            )



        group by
            user_id, courserun_readable_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

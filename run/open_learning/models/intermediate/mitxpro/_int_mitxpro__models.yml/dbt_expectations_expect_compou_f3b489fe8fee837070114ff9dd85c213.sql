select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            coursetopic_name
            , course_id
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__course_to_topics
        where
            1 = 1
            and
            not (
                coursetopic_name is null
                and course_id is null

            )



        group by
            coursetopic_name, course_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

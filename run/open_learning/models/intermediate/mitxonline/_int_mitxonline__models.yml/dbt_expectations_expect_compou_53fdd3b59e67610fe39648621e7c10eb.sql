select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            course_id
            , program_id
            , user_id
        from
            ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_flexiblepriceapplication
        where
            1 = 1
            and
            not (
                course_id is null
                and program_id is null
                and user_id is null

            )



        group by
            course_id, program_id, user_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

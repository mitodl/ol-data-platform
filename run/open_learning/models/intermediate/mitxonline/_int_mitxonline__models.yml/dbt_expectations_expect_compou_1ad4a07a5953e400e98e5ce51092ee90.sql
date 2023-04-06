select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            blockedcountry_code
            , course_id
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__course_blockedcountries
        where
            1 = 1
            and
            not (
                blockedcountry_code is null
                and course_id is null

            )



        group by
            blockedcountry_code, course_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

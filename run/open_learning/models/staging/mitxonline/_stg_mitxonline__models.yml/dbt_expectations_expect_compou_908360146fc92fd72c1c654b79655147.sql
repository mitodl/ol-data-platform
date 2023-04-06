select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            program_id
            , programrequirement_depth
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_programrequirement
        where
            1 = 1
            and
            programrequirement_depth = 1

            and not (
                program_id is null
                and programrequirement_depth is null

            )



        group by
            program_id, programrequirement_depth
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

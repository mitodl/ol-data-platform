select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            line_id
            , courserun_id
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_linerunselection
        where
            1 = 1
            and
            not (
                line_id is null
                and courserun_id is null

            )



        group by
            line_id, courserun_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

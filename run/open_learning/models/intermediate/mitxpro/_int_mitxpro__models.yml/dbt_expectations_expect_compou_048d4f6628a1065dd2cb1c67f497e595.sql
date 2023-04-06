select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            user_id
            , program_id
            , ecommerce_order_id
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__programenrollments
        where
            1 = 1
            and
            not (
                user_id is null
                and program_id is null
                and ecommerce_order_id is null

            )



        group by
            user_id, program_id, ecommerce_order_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

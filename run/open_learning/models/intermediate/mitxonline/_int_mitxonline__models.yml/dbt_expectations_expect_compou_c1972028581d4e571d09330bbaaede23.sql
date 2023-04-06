select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            product_type
            , programrun_id
            , courserun_id
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__ecommerce_product
        where
            1 = 1
            and
            product_is_active

            and not (
                product_type is null
                and programrun_id is null
                and courserun_id is null

            )



        group by
            product_type, programrun_id, courserun_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            couponversion_id
            , order_id
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_couponredemption
        where
            1 = 1
            and
            not (
                couponversion_id is null
                and order_id is null

            )



        group by
            couponversion_id, order_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

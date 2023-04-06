select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with validation_errors as (

        select
            coupon_id
            , product_id
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_couponproduct
        where
            1 = 1
            and
            not (
                coupon_id is null
                and product_id is null

            )



        group by
            coupon_id, product_id
        having count(*) > 1

    )

    select * from validation_errors


) as dbt_internal_test

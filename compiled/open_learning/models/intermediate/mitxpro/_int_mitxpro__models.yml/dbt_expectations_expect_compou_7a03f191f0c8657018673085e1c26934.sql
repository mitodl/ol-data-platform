with validation_errors as (

    select
        coupon_id
        , product_id
    from dev.main_intermediate.int__mitxpro__ecommerce_couponproduct
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

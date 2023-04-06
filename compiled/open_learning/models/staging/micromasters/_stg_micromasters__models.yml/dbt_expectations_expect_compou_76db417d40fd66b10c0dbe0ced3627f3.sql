with validation_errors as (

    select
        order_id
        , coupon_id
    from dev.main_staging.stg__micromasters__app__postgres__ecommerce_redeemedcoupon
    where
        1 = 1
        and
        not (
            order_id is null
            and coupon_id is null

        )



    group by
        order_id, coupon_id
    having count(*) > 1

)

select * from validation_errors

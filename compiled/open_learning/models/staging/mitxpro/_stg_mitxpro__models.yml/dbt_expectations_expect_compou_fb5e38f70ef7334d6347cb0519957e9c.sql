with validation_errors as (

    select
        couponversion_id
        , order_id
    from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_couponredemption
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

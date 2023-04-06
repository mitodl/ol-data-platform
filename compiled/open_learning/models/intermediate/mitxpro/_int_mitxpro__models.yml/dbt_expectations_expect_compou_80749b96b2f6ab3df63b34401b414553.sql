with validation_errors as (

    select
        user_id
        , courserun_id
        , ecommerce_order_id
    from dev.main_intermediate.int__mitxpro__courserunenrollments
    where
        1 = 1
        and
        not (
            user_id is null
            and courserun_id is null
            and ecommerce_order_id is null

        )



    group by
        user_id, courserun_id, ecommerce_order_id
    having count(*) > 1

)

select * from validation_errors

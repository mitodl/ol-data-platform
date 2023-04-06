with validation_errors as (

    select
        basket_id
        , courserun_id
    from dev.main_intermediate.int__mitxpro__ecommerce_basketrunselection
    where
        1 = 1
        and
        not (
            basket_id is null
            and courserun_id is null

        )



    group by
        basket_id, courserun_id
    having count(*) > 1

)

select * from validation_errors

with validation_errors as (

    select
        product_type
        , program_id
        , courserun_id
    from dev.main_intermediate.int__mitxpro__ecommerce_product
    where
        1 = 1
        and
        not (
            product_type is null
            and program_id is null
            and courserun_id is null

        )



    group by
        product_type, program_id, courserun_id
    having count(*) > 1

)

select * from validation_errors

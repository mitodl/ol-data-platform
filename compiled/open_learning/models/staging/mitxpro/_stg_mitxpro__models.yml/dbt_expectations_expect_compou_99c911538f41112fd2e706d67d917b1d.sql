with validation_errors as (

    select
        contenttype_id
        , product_object_id
    from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_product
    where
        1 = 1
        and
        not (
            contenttype_id is null
            and product_object_id is null

        )



    group by
        contenttype_id, product_object_id
    having count(*) > 1

)

select * from validation_errors

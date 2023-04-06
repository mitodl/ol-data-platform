with validation_errors as (

    select
        product_object_id
        , contenttype_id
    from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_product
    where
        1 = 1
        and
        product_is_active

        and not (
            product_object_id is null
            and contenttype_id is null

        )



    group by
        product_object_id, contenttype_id
    having count(*) > 1

)

select * from validation_errors

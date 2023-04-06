with validation_errors as (

    select
        courseware_object_id
        , contenttype_id
        , user_id
    from dev.main_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepriceapplication
    where
        1 = 1
        and
        not (
            courseware_object_id is null
            and contenttype_id is null
            and user_id is null

        )



    group by
        courseware_object_id, contenttype_id, user_id
    having count(*) > 1

)

select * from validation_errors

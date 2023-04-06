with validation_errors as (

    select
        course_id
        , program_id
        , user_id
    from dev.main_intermediate.int__mitxonline__flexiblepricing_flexiblepriceapplication
    where
        1 = 1
        and
        not (
            course_id is null
            and program_id is null
            and user_id is null

        )



    group by
        course_id, program_id, user_id
    having count(*) > 1

)

select * from validation_errors

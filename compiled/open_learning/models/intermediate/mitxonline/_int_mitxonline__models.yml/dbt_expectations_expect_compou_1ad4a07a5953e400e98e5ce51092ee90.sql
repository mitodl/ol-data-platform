with validation_errors as (

    select
        blockedcountry_code
        , course_id
    from dev.main_intermediate.int__mitxonline__course_blockedcountries
    where
        1 = 1
        and
        not (
            blockedcountry_code is null
            and course_id is null

        )



    group by
        blockedcountry_code, course_id
    having count(*) > 1

)

select * from validation_errors

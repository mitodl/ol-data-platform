with validation_errors as (

    select
        blockedcountry_code
        , course_id
    from dev.main_staging.stg__mitxonline__app__postgres__courses_blockedcountry
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

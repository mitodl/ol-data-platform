with validation_errors as (

    select
        course_id
        , electiveset_id
    from dev.main_staging.stg__micromasters__app__postgres__courses_electiveset_to_course
    where
        1 = 1
        and
        not (
            course_id is null
            and electiveset_id is null

        )



    group by
        course_id, electiveset_id
    having count(*) > 1

)

select * from validation_errors

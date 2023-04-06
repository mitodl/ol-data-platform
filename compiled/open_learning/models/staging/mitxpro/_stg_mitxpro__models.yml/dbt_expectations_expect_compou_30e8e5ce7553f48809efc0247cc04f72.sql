with validation_errors as (

    select
        coursetopic_id
        , course_id
    from dev.main_staging.stg__mitxpro__app__postgres__courses_course_to_topic
    where
        1 = 1
        and
        not (
            coursetopic_id is null
            and course_id is null

        )



    group by
        coursetopic_id, course_id
    having count(*) > 1

)

select * from validation_errors

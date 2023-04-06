with validation_errors as (

    select
        course_id
        , courserun_tag
    from dev.main_staging.stg__mitxpro__app__postgres__courses_courserun
    where
        1 = 1
        and
        not (
            course_id is null
            and courserun_tag is null

        )



    group by
        course_id, courserun_tag
    having count(*) > 1

)

select * from validation_errors

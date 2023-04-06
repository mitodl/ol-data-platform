with validation_errors as (

    select
        user_id
        , platform
        , courserun_readable_id
    from dev.main_intermediate.int__micromasters__course_enrollments
    where
        1 = 1
        and
        not (
            user_id is null
            and platform is null
            and courserun_readable_id is null

        )



    group by
        user_id, platform, courserun_readable_id
    having count(*) > 1

)

select * from validation_errors

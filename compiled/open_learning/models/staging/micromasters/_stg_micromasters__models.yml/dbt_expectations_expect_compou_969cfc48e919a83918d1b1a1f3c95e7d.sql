with validation_errors as (

    select
        program_id
        , course_position_in_program
    from dev.main_staging.stg__micromasters__app__postgres__courses_course
    where
        1 = 1
        and
        not (
            program_id is null
            and course_position_in_program is null

        )



    group by
        program_id, course_position_in_program
    having count(*) > 1

)

select * from validation_errors

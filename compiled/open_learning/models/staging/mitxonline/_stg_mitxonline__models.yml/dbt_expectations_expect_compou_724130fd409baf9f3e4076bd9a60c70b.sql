with validation_errors as (

    select
        program_id
        , user_id
    from dev.main_staging.stg__mitxonline__app__postgres__courses_programenrollment
    where
        1 = 1
        and
        not (
            program_id is null
            and user_id is null

        )



    group by
        program_id, user_id
    having count(*) > 1

)

select * from validation_errors

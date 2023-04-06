with validation_errors as (

    select
        program_id
        , programrun_tag
    from dev.main_staging.stg__mitxpro__app__postgres__courses_programrun
    where
        1 = 1
        and
        not (
            program_id is null
            and programrun_tag is null

        )



    group by
        program_id, programrun_tag
    having count(*) > 1

)

select * from validation_errors

with validation_errors as (

    select
        program_id
        , programrequirement_depth
    from dev.main_staging.stg__mitxonline__app__postgres__courses_programrequirement
    where
        1 = 1
        and
        programrequirement_depth = 1

        and not (
            program_id is null
            and programrequirement_depth is null

        )



    group by
        program_id, programrequirement_depth
    having count(*) > 1

)

select * from validation_errors

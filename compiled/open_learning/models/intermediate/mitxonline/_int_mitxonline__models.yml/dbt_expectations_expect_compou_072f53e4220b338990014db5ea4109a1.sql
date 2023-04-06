with validation_errors as (

    select
        user_id
        , program_id
    from dev.main_intermediate.int__mitxonline__programenrollments
    where
        1 = 1
        and
        not (
            user_id is null
            and program_id is null

        )



    group by
        user_id, program_id
    having count(*) > 1

)

select * from validation_errors

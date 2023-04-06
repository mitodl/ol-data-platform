with validation_errors as (

    select
        courserun_readable_id
        , user_id
    from dev.main_staging.stg__mitxresidential__openedx__courserun_enrollment
    where
        1 = 1
        and
        not (
            courserun_readable_id is null
            and user_id is null

        )



    group by
        courserun_readable_id, user_id
    having count(*) > 1

)

select * from validation_errors

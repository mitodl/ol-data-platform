with validation_errors as (

    select
        courserun_id
        , user_id
    from dev.main_staging.stg__mitxonline__app__postgres__courses_courserunenrollment
    where
        1 = 1
        and
        not (
            courserun_id is null
            and user_id is null

        )



    group by
        courserun_id, user_id
    having count(*) > 1

)

select * from validation_errors

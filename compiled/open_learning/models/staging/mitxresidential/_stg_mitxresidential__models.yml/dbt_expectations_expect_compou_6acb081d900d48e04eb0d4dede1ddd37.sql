with validation_errors as (

    select
        courserun_readable_id
        , user_id
        , courseaccessrole_role
    from dev.main_staging.stg__mitxresidential__openedx__user_courseaccessrole
    where
        1 = 1
        and
        not (
            courserun_readable_id is null
            and user_id is null
            and courseaccessrole_role is null

        )



    group by
        courserun_readable_id, user_id, courseaccessrole_role
    having count(*) > 1

)

select * from validation_errors

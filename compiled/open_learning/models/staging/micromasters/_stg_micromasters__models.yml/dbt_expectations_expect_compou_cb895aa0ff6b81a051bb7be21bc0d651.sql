with validation_errors as (

    select
        user_username
        , user_auth_provider
    from dev.main_staging.stg__micromasters__app__postgres__auth_usersocialauth
    where
        1 = 1
        and
        not (
            user_username is null
            and user_auth_provider is null

        )



    group by
        user_username, user_auth_provider
    having count(*) > 1

)

select * from validation_errors

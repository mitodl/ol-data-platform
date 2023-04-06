with validation_errors as (

    select
        user_id
        , platform
    from dev.main_intermediate.int__mitx__users
    where
        1 = 1
        and
        not (
            user_id is null
            and platform is null

        )



    group by
        user_id, platform
    having count(*) > 1

)

select * from validation_errors

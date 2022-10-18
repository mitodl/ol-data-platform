with users as (
    select
        user_id
        , user_username
        , user_email
    from {{ ref('stg__bootcamps__app__postgres__auth_user') }}
)

select * from users

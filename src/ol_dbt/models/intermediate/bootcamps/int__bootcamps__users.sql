with users as (
    select
        id
        , username
        , email
    from {{ ref('stg__bootcamps__app__postgres__auth_user') }}
)

select * from users

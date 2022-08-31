with users as (
    select
        id
        , username
        , email
    from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)

select * from users

-- MITx Online User Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__users_user') }}
)

, cleaned as (

    select
        id
        , username
        , name as full_name
        , email as user_email
        , created_on as user_joined_on_utc
        , last_login as last_login_utc
        , (is_superuser or is_staff) as is_open_learning_staff
    from source
    where is_active = true


)

select * from cleaned

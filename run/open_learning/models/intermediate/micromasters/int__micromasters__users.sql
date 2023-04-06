create table ol_data_lake_qa.ol_warehouse_qa_intermediate.int__micromasters__users__dbt_tmp

as (
    with users as (
        select * from ol_data_lake_qa.ol_warehouse_qa_staging.stg__micromasters__app__postgres__auth_user
    )

    , profiles as (
        select * from ol_data_lake_qa.ol_warehouse_qa_staging.stg__micromasters__app__postgres__profiles_profile
    )

    , mitxonline_auth as (
        select
            user_id
            , user_username as user_mitxonline_username
        from ol_data_lake_qa.ol_warehouse_qa_staging.stg__micromasters__app__postgres__auth_usersocialauth
        where user_auth_provider = 'mitxonline'
    )

    , edxorg_auth as (
        select
            user_id
            , user_username as user_edxorg_username
        from ol_data_lake_qa.ol_warehouse_qa_staging.stg__micromasters__app__postgres__auth_usersocialauth
        where user_auth_provider = 'edxorg'
    )


    select
        users.user_id
        , users.user_username
        , users.user_email
        , users.user_joined_on
        , users.user_last_login
        , users.user_is_active
        , profiles.user_full_name
        , profiles.user_address_country
        , mitxonline_auth.user_mitxonline_username
        , edxorg_auth.user_edxorg_username
    from users
    left join profiles on profiles.user_id = users.user_id
    left join mitxonline_auth on mitxonline_auth.user_id = users.user_id
    left join edxorg_auth on edxorg_auth.user_id = users.user_id
);

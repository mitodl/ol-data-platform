create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__users_user__dbt_tmp

as (
    -- xPro User Information

    with source as (
        select * from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__users_user
    )

    , cleaned as (
        select
            id as user_id
            , username as user_username
            , email as user_email
            , is_active as user_is_active
            , name as user_full_name
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as user_joined_on
            ,
            to_iso8601(from_iso8601_timestamp(last_login))
            as user_last_login
        from source
    )

    select * from cleaned
);

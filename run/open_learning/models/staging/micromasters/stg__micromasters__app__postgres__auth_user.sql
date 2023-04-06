create table ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__auth_user__dbt_tmp

as (
    -- MicroMasters User Information

    with source as (
        select * from ol_data_lake_production.ol_warehouse_production_raw.raw__micromasters__app__postgres__auth_user
    )

    , cleaned as (

        select
            id as user_id
            , username as user_username
            , email as user_email
            , is_active as user_is_active
            ,
            to_iso8601(from_iso8601_timestamp(date_joined))
            as user_joined_on
            ,
            to_iso8601(from_iso8601_timestamp(last_login))
            as user_last_login
        from source
    )

    select * from cleaned
);

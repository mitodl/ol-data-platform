

    create table ol_data_lake_qa.ol_warehouse_qa_staging.stg__bootcamps__app__postgres__auth_user__dbt_tmp
    WITH (format = 'PARQUET')
  as (
    with source as (
    select * from ol_data_lake_qa.ol_warehouse_qa_raw.raw__bootcamps__app__postgres__auth_user
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

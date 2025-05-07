with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__tables__auth_user') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'id') }}
, cleaned as (
    select
        ---all values are ingested as string, so we need to cast here to match other data sources
        cast(id as integer) as user_id
        , username as user_username
        , email as user_email
        , cast(is_active as boolean) as user_is_active
        , cast(is_staff as boolean) as user_is_staff
        , cast(is_superuser as boolean) as user_is_superuser
        , to_iso8601(from_iso8601_timestamp_nanos(
            regexp_replace(date_joined, '(\d{4}-\d{2}-\d{2})[ ](\d{2}:\d{2}:\d{2})(.*?)', '$1T$2$3')
        )) as user_joined_on
        , case
            when lower(last_login) = 'null' or lower(last_login) = 'none' then null
            else to_iso8601(from_iso8601_timestamp_nanos(
                regexp_replace(last_login, '(\d{4}-\d{2}-\d{2})[ ](\d{2}:\d{2}:\d{2})(.*?)', '$1T$2$3')
            ))
        end as user_last_login
    from most_recent_source
)

select * from cleaned

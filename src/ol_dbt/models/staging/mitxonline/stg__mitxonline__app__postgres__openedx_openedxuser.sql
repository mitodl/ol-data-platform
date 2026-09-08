with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__openedx_openedxuser') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, cleaned as (

    select
        id as openedxuser_id
        , user_id
        , platform as openedxuser_platform
        , edx_username as openedxuser_username
        , desired_edx_username as openedxuser_desired_username
        , has_been_synced as openedxuser_has_been_synced
        , {{ cast_timestamp_to_iso8601('created_on') }} as openedxuser_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as openedxuser_updated_on
    from most_recent_source
)

select * from cleaned

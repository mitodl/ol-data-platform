with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__program') }}
)

{{ deduplicate_raw_table(order_by='retrieved_at' , partition_columns = 'uuid') }}

, cleaned as (
    select
        uuid as program_uuid
        , title as program_title
        , subtitle as program_subtitle
        , type as program_type
        , status as program_status
        , authoring_organizations as program_organization
        , {{ cast_timestamp_to_iso8601('data_modified_timestamp') }} as program_updated_on
    from most_recent_source
)

select * from cleaned

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__thirdparty__zendesk_support__brands') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'id') }}
, cleaned as (
    select
        id as brand_id
        , url as brand_api_url
        , brand_url
        , name as brand_name
        , active as brand_is_active
        , default as brand_is_default
        , subdomain as brand_subdomain
        , is_deleted as brand_is_deleted
        , has_help_center as brand_has_help_center
        , help_center_state as brand_help_center_state
        , {{ cast_timestamp_to_iso8601('created_at') }} as brand_created_at
        , {{ cast_timestamp_to_iso8601('updated_at') }} as brand_updated_at
    from most_recent_source
)

select * from cleaned

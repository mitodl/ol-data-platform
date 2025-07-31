with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__thirdparty__zendesk_support__organizations') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'id') }}
, cleaned as (
    select
        id as organization_id
        , url as organization_api_url
        , name as organization_name
        , nullif(tags, array[]) as organization_tags
        , nullif(notes, '') as organization_notes
        , group_id
        , nullif(domain_names, array[]) as organization_domain_names
        , shared_tickets as organization_has_shared_tickets
        , shared_comments as organization_has_shared_comments
        , {{ cast_timestamp_to_iso8601('deleted_at') }} as organization_deleted_at
        , {{ cast_timestamp_to_iso8601('created_at') }} as organization_created_at
        , {{ cast_timestamp_to_iso8601('updated_at') }} as organization_updated_at
    from most_recent_source
)

select * from cleaned

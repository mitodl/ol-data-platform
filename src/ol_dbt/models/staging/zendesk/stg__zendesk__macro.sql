with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__thirdparty__zendesk_support__macros') }}
)

{{ deduplicate_raw_table(raw_table='raw__thirdparty__zendesk_support__macros', partition_columns = 'id') }}
, cleaned as (
    select
        id as macro_id
        , url as macro_api_url
        , title as macro_title
        , raw_title as macro_raw_title
        , description as macro_description
        , active as macro_is_active
        , {{ adapter.quote("default") }} as macro_is_default
        , position as macro_position
        , actions as macro_actions
        , nullif({{ json_query_string('restriction', "'$.type'") }}, 'null') as macro_restriction_type
        , {{ json_extract_value('restriction', "'$.ids'") }} as macro_restriction_group_ids
        , {{ cast_timestamp_to_iso8601('created_at') }} as macro_created_at
        , {{ cast_timestamp_to_iso8601('updated_at') }} as macro_updated_at
    from most_recent_source
)

select * from cleaned

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__wagtailcore_revision') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, cleaned as (
    select
        id as wagtailcore_revision_id,
        content as wagtailcore_revision_content,
        user_id,
        object_id as wagtail_page_id,
        object_str as wagtail_page_title,
        content_type_id as contenttype_id,
        base_content_type_id as base_contenttype_id,
        {{ cast_timestamp_to_iso8601('created_at') }} as wagtailcore_revision_created_on
    from most_recent_source
)

select * from cleaned

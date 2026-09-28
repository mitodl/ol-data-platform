with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__content_feedback_contentfeedback') }}
)

{{ deduplicate_raw_table(order_by='updated_on', partition_columns='id') }}

, cleaned as (
    select
        id as contentfeedback_id
        , user_id
        , course_id as courserun_readable_id
        , nullif(course_name, '') as courserun_title
        , block_usage_key as contentfeedback_block_usage_key
        , nullif(block_type, '') as contentfeedback_block_type
        , nullif(block_display_name, '') as contentfeedback_block_display_name
        , nullif(unit_title, '') as contentfeedback_unit_title
        , nullif(url, '') as contentfeedback_url
        , sentiment as contentfeedback_sentiment
        , nullif(trim(comment), '') as contentfeedback_comment
        , {{ cast_timestamp_to_iso8601('created_on') }} as contentfeedback_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as contentfeedback_updated_on
    from most_recent_source
)

select * from cleaned

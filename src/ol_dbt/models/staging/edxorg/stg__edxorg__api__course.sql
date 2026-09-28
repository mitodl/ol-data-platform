with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__mitx_course') }}
)

{{ deduplicate_raw_table(order_by='retrieved_at' , partition_columns = 'course_key') }}
, cleaned as (
    select
        course_key as course_readable_id
        , title as course_title
        , short_description as course_description
        , full_description as course_full_description
        , level_type as course_level
        , course_type
        , owner as course_organizations
        , marketing_url as course_marketing_url
        , {{ json_query_string('image', "'$.url'") }} as course_image_url
        , if(
            subjects = '[]'
            , null
            , {{ json_array_field_values('subjects', 'name') }}
        ) as course_topics
        , prerequisites_raw as course_prerequisites_text
        , {{ cast_timestamp_to_iso8601('modified') }} as course_updated_at
        -- when the catalog extraction that wrote this row ran; courses in the latest
        -- extraction are the ones the catalog currently lists
        , retrieved_at as course_retrieved_at

    from most_recent_source
)

select * from cleaned

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_coursepage') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__cms_coursepage', partition_columns='page_ptr_id') }}

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , course_id
        , description as course_description
        , length as course_length
        , effort as course_effort
        , prerequisites as course_prerequisites
        , faq_url as course_faq_url
        , about as course_about
        , what_you_learn as course_what_you_learn
        , video_url as course_video_url
        , {{ json_query_string('price', "'$[0].value.text'") }} as course_price
        , feature_image_id as course_feature_image_id
        , min_weeks as course_min_weeks
        , max_weeks as course_max_weeks
        , min_weekly_hours as course_min_weekly_hours
        , max_weekly_hours as course_max_weekly_hours
        , min_price as course_min_price
        , max_price as course_max_price
        , include_in_learn_catalog as course_is_in_learn_catalog
        , ingest_content_files_for_ai as course_ingests_content_files_for_ai
    from most_recent_source
)

select * from cleaned

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_programpage') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__cms_programpage', partition_columns='page_ptr_id') }}

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , program_id
        , description as program_description
        , length as program_length
        , effort as program_effort
        , prerequisites as program_prerequisites
        , faq_url as program_faq_url
        , about as program_about
        , what_you_learn as program_what_you_learn
        , video_url as program_video_url
        , {{ json_query_string('price', "'$[0].value.text'") }} as program_price
        , feature_image_id as program_feature_image_id
        , min_weeks as program_min_weeks
        , max_weeks as program_max_weeks
        , min_weekly_hours as program_min_weekly_hours
        , max_weekly_hours as program_max_weekly_hours
        , min_price as program_min_price
        , max_price as program_max_price
        , include_in_learn_catalog as program_is_in_learn_catalog
    from most_recent_source
)

select * from cleaned

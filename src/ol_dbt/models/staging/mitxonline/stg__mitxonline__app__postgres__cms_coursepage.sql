with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_coursepage') }}
)

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , course_id
        , description as course_description
        , length as course_length
        , effort as course_effort
        , price as course_price_detail
        , prerequisites as course_prerequisites
        , faq_url as course_faq_url
        , about as course_about
        , what_you_learn as course_what_you_learn
        , video_url as course_video_url
    from source
)

select * from cleaned

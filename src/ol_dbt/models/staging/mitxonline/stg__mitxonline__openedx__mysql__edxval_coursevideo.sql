with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__openedx__mysql__edxval_coursevideo') }}
)

, cleaned as (

    select
        id as coursevideo_id
        , course_id as courserun_readable_id
        , video_id
        , is_hidden as coursevideo_is_hidden

    from source
)

select * from cleaned

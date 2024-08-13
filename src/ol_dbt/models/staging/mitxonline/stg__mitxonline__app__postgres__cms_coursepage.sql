with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_coursepage') }}
)

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , description as cms_coursepage_description
        , course_id
        , length as cms_coursepage_length
        , effort as cms_coursepage_effort
        , prerequisites as cms_coursepage_prerequisites
    from source
)

select * from cleaned

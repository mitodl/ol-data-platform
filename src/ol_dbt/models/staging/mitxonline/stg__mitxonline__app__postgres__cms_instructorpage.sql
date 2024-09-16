with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_instructorpage') }}
)

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , instructor_name
        , instructor_title
        , instructor_bio_short
        , instructor_bio_long
    from source
)

select * from cleaned

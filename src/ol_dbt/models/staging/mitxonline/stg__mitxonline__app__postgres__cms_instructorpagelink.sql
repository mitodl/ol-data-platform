with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_instructorpagelink') }}
)

, cleaned as (
    select
        page_id as wagtail_page_id
        , linked_instructor_page_id as instructor_wagtail_page_id
    from source
)

select * from cleaned

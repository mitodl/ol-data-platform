with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_instructorpage') }}
)

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , instructor_name as cms_instructorpage_instructor_name
        , instructor_title as cms_instructorpage_instructor_title
        , instructor_bio_long as cms_instructorpage_instructor_bio_long
    from source
)

select * from cleaned

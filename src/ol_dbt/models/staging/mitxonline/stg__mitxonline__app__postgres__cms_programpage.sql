with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_programpage') }}
)

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , description as cms_programpage_description
        , program_id
        , length as cms_programpage_length
        , effort as cms_programpage_effort
    from source
)

select * from cleaned

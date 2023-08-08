with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_facultymemberspage') }}
)

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , heading as cms_facultymemberspage_heading
        , subhead as cms_facultymemberspage_subhead
        , members as cms_facultymemberspage_faculty
    from source
)

select * from cleaned

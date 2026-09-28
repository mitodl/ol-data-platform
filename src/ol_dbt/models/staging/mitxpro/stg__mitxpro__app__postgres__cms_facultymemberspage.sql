with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_facultymemberspage') }}
)

{{ deduplicate_raw_table(raw_table='raw__xpro__app__postgres__cms_facultymemberspage', partition_columns='page_ptr_id') }}
, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , heading as cms_facultymemberspage_heading
        , subhead as cms_facultymemberspage_subhead
        , members as cms_facultymemberspage_faculty
    from most_recent_source
)

select * from cleaned

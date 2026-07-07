with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_facultymemberspage') }}
)

, source_deduped as (
    select
        *
        , row_number() over (partition by page_ptr_id order by _airbyte_extracted_at desc) as row_num
    from source
)

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , heading as cms_facultymemberspage_heading
        , subhead as cms_facultymemberspage_subhead
        , members as cms_facultymemberspage_faculty
    from source_deduped
    where row_num = 1
)

select * from cleaned

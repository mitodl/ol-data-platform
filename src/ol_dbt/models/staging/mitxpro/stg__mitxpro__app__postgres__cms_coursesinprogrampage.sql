with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_coursesinprogrampage') }}
)

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , heading as cms_coursesinprogrampage_heading
        , body as cms_coursesinprogrampage_body
        , cast(json_parse(json_query(contents, 'lax $[*].value' with array wrapper)) as array(integer)) --noqa
            as cms_coursesinprogrampage_coursepage_wagtail_page_ids
    from source
)

select * from cleaned

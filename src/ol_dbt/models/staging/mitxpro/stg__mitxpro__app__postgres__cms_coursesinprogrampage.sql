with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_coursesinprogrampage') }}
)

{{ deduplicate_raw_table(raw_table='raw__xpro__app__postgres__cms_coursesinprogrampage', partition_columns = 'page_ptr_id') }}

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , heading as cms_coursesinprogrampage_heading
        , body as cms_coursesinprogrampage_body
        , {{ json_array_field_values('contents', 'value', 'integer') }}
            as cms_coursesinprogrampage_coursepage_wagtail_page_ids
    from most_recent_source
)

select * from cleaned

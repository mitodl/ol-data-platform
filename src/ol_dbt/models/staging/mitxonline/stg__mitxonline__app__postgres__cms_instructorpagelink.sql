with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_instructorpagelink') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__cms_instructorpagelink', partition_columns='id') }}

, cleaned as (
    select
        page_id as wagtail_page_id
        , linked_instructor_page_id as instructor_wagtail_page_id
    from most_recent_source
)

select * from cleaned

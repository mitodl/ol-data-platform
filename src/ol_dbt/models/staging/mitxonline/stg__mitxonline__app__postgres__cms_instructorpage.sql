with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_instructorpage') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='page_ptr_id') }}

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , instructor_name
        , instructor_title
        , instructor_bio_short
        , instructor_bio_long
    from most_recent_source
)

select * from cleaned

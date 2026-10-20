with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__cms_productpagefaq') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__cms_productpagefaq', partition_columns='id') }}

, cleaned as (
    select
        id as productpagefaq_id
        , page_id as wagtail_page_id
        , question as productpagefaq_question
        , answer as productpagefaq_answer
        , sort_order as productpagefaq_sort_order
    from most_recent_source
)

select * from cleaned

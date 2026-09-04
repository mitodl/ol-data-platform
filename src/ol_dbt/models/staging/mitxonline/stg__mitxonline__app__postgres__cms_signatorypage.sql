with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__cms_signatorypage') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='page_ptr_id') }}

, cleaned as (
    select
        name as signatorypage_name,
        title_1 as signatorypage_title_1,
        title_2 as signatorypage_title_2,
        title_3 as signatorypage_title_3,
        page_ptr_id as wagtail_page_id,
        organization as signatorypage_organization,
        signature_image_id as wagtailimages_image_id
    from most_recent_source
)

select * from cleaned

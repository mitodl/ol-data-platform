with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__cms_signatorypage') }}
)

, cleaned as (
    select
        name as signatorypage_name,
        title_1 as signatorypage_title_1,
        title_2 as signatorypage_title_2,
        title_3 as signatorypage_title_3,
        page_ptr_id as wagtail_page_id,
        organization as organization,
        signature_image_id as wagtailimages_image_id
    from source
)

select *from cleaned

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__cms_programpage') }}
)

, cleaned as (
    select
        page_ptr_id                     as wagtail_page_id
        , program_id
        , thumbnail_image_id            as cms_programpage_thumbnail_image_id
        , background_image_id           as cms_programpage_background_image_id
        , program_letter_logo_id        as cms_programpage_letter_logo_id
        , description                   as cms_programpage_description
        , faculty_description           as cms_programpage_faculty_description
        , title_over_image              as cms_programpage_title_over_image
        , program_home_page_url         as cms_programpage_home_page_url
        , title_program_home_page_url   as cms_programpage_home_page_url_title
        , program_contact_email         as cms_programpage_contact_email
        , program_subscribe_link        as cms_programpage_subscribe_link
    from source
)

select * from cleaned

-- MicroMasters Program Information

with programs as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_program') }}
)

, cms_pages as (
    select * from {{ ref('stg__micromasters__app__postgres__cms_programpage') }}
)

, wagtail_pages as (
    select * from {{ ref('stg__micromasters__app__postgres__wagtailcore_page') }}
)

, program_images as (
    select * from {{ ref('stg__micromasters__app__postgres__wagtailimages_image') }}
)

select
    programs.program_id
    , programs.program_title
    , programs.program_description
    , programs.program_is_live
    , programs.program_num_required_courses
    , wagtail_pages.wagtail_page_live_url          as program_page_url
    , program_images.image_url                     as program_image_url
from programs
left join cms_pages
    on programs.program_id = cms_pages.program_id
left join wagtail_pages
    on cms_pages.wagtail_page_id = wagtail_pages.wagtail_page_id
left join program_images
    on cms_pages.cms_programpage_thumbnail_image_id = program_images.image_id

-- Program information for MITx Online

with programs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_program') }}
)

, program_pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_programpage') }}
)

, wagtail_page as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_wagtail_page') }}
)

select
    programs.program_id
    , programs.program_title
    , programs.program_is_live
    , programs.program_readable_id
    , programs.program_type
    , programs.program_is_dedp
    , programs.program_is_micromasters
    , program_pages.program_description
    , program_pages.program_price
    , program_pages.program_length
    , program_pages.program_effort
    , program_pages.program_prerequisites
    , program_pages.program_about
    , program_pages.program_what_you_learn
    , wagtail_page.wagtail_page_slug as program_page_slug
    , wagtail_page.wagtail_page_url_path as program_page_url_path
    , wagtail_page.wagtail_page_is_live as program_page_is_live
    , wagtail_page.wagtail_page_first_published_on as program_page_first_published_on
    , wagtail_page.wagtail_page_last_published_on as program_page_last_published_on
from programs
left join program_pages on programs.program_id = program_pages.program_id
left join wagtail_page on program_pages.wagtail_page_id = wagtail_page.wagtail_page_id

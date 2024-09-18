-- Program information for MITxPro

with programs as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_program') }}
)

, cms_programs as (
    select * from {{ ref('stg__mitxpro__app__postgres__cms_programpage') }}
)

, wagtail_page as (
    select * from {{ ref('stg__mitxpro__app__postgres__wagtail_page') }}
)

, platform as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_platform') }}
)

select
    programs.program_id
    , programs.program_title
    , programs.program_is_live
    , programs.program_readable_id
    , programs.program_is_external
    , platform.platform_name
    , cms_programs.cms_programpage_description
    , cms_programs.cms_programpage_subhead
    , cms_programs.cms_programpage_catalog_details
    , cms_programs.cms_programpage_duration
    , cms_programs.cms_programpage_format
    , cms_programs.cms_programpage_time_commitment
    , wagtail_page.wagtail_page_slug as cms_programpage_slug
    , wagtail_page.wagtail_page_url_path as cms_programpage_url_path
    , wagtail_page.wagtail_page_is_live as cms_programpage_is_live
    , wagtail_page.wagtail_page_first_published_on as cms_programpage_first_published_on
    , wagtail_page.wagtail_page_last_published_on as cms_programpage_last_published_on
from programs
left join cms_programs
    on programs.program_id = cms_programs.program_id
left join wagtail_page
    on cms_programs.wagtail_page_id = wagtail_page.wagtail_page_id
left join platform
    on programs.platform_id = platform.platform_id

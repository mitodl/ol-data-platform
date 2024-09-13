-- Course information for MITxPro

with courses as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_course') }}
)

, cms_courses as (
    select * from {{ ref('stg__mitxpro__app__postgres__cms_coursepage') }}
)

, wagtail_page as (
    select * from {{ ref('stg__mitxpro__app__postgres__wagtail_page') }}
)

, platform as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_platform') }}
)

select
    courses.course_id
    , courses.program_id
    , courses.course_title
    , courses.course_is_live
    , courses.course_readable_id
    , courses.course_is_external
    , courses.short_program_code
    , platform.platform_name
    , cms_courses.cms_coursepage_description
    , cms_courses.cms_coursepage_subhead
    , cms_courses.cms_coursepage_catalog_details
    , cms_courses.cms_coursepage_duration
    , cms_courses.cms_coursepage_format
    , cms_courses.cms_coursepage_time_commitment
    , wagtail_page.wagtail_page_slug as cms_coursepage_slug
    , wagtail_page.wagtail_page_url_path as cms_coursepage_url_path
    , wagtail_page.wagtail_page_is_live as cms_coursepage_is_live
    , wagtail_page.wagtail_page_first_published_on as cms_coursepage_first_published_on
    , wagtail_page.wagtail_page_last_published_on as cms_coursepage_last_published_on
from courses
left join cms_courses
    on courses.course_id = cms_courses.course_id
left join wagtail_page
    on cms_courses.wagtail_page_id = wagtail_page.wagtail_page_id
left join platform
    on courses.platform_id = platform.platform_id

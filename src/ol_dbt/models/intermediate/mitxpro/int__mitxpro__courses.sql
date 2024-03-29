-- Course information for MITxPro

with courses as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_course') }}
)

, cms_courses as (
    select * from {{ ref('stg__mitxpro__app__postgres__cms_coursepage') }}
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

from courses
left join cms_courses
    on courses.course_id = cms_courses.course_id
left join platform
    on courses.platform_id = platform.platform_id

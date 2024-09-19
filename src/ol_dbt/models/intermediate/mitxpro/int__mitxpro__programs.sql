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

, course_program as (
    select * from {{ ref('int__mitxpro__coursesinprogram') }}
)

, course_topics as (
    select * from {{ ref('int__mitxpro__courses_to_topics') }}
)

, certificate_page as (
    select * from {{ ref('stg__mitxpro__app__postgres__cms_certificatepage') }}
)

, certificate_page_path as (
    select
        certificate_page.wagtail_page_id
        , certificate_page.cms_certificate_ceus
        , wagtail_page.wagtail_page_path
    from certificate_page
    inner join wagtail_page
        on certificate_page.wagtail_page_id = wagtail_page.wagtail_page_id
)

, program_topics as (
    select
        course_program.program_id
        , array_join(array_agg(course_topics.coursetopic_name), ', ') as program_topics
    from course_topics
    inner join course_program on course_topics.course_id = course_program.course_id
    group by course_program.program_id
)

, program_instructors as (
    select
        program_id
        , array_join(array_agg(cms_facultymemberspage_facultymember_name), ', ') as program_instructors
    from {{ ref('int__mitxpro__programsfaculty') }}
    group by program_id
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
    , certificate_page_path.cms_certificate_ceus
    , program_topics.program_topics
    , program_instructors.program_instructors
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
left join certificate_page_path
    on wagtail_page.wagtail_page_path like certificate_page_path.wagtail_page_path || '%'
left join program_topics on programs.program_id = program_topics.program_id
left join program_instructors on programs.program_id = program_instructors.program_id

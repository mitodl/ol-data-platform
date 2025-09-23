-- Course information for MITxPro
with
    courses as (select * from {{ ref("stg__mitxpro__app__postgres__courses_course") }}),
    cms_courses as (select * from {{ ref("stg__mitxpro__app__postgres__cms_coursepage") }}),
    wagtail_page as (select * from {{ ref("stg__mitxpro__app__postgres__wagtail_page") }}),
    platform as (select * from {{ ref("stg__mitxpro__app__postgres__courses_platform") }}),
    certificate_page as (select * from {{ ref("stg__mitxpro__app__postgres__cms_certificatepage") }}),
    certificate_page_path as (
        select certificate_page.wagtail_page_id, certificate_page.cms_certificate_ceus, wagtail_page.wagtail_page_path
        from certificate_page
        inner join wagtail_page on certificate_page.wagtail_page_id = wagtail_page.wagtail_page_id
    ),
    course_topics as (
        select course_id, array_join(array_agg(coursetopic_name), ', ') as course_topics
        from {{ ref("int__mitxpro__courses_to_topics") }}
        group by course_id
    ),
    course_instructors as (
        select course_id, array_join(array_agg(cms_facultymemberspage_facultymember_name), ', ') as course_instructors
        from {{ ref("int__mitxpro__coursesfaculty") }}
        group by course_id
    )

select
    courses.course_id,
    courses.program_id,
    courses.course_title,
    courses.course_is_live,
    courses.course_readable_id,
    courses.course_is_external,
    courses.short_program_code,
    platform.platform_name,
    cms_courses.cms_coursepage_description,
    cms_courses.cms_coursepage_subhead,
    cms_courses.cms_coursepage_catalog_details,
    cms_courses.cms_coursepage_duration,
    cms_courses.cms_coursepage_format,
    cms_courses.cms_coursepage_time_commitment,
    certificate_page_path.cms_certificate_ceus,
    course_topics.course_topics,
    course_instructors.course_instructors,
    wagtail_page.wagtail_page_slug as cms_coursepage_slug,
    wagtail_page.wagtail_page_url_path as cms_coursepage_url_path,
    wagtail_page.wagtail_page_is_live as cms_coursepage_is_live,
    wagtail_page.wagtail_page_first_published_on as cms_coursepage_first_published_on,
    wagtail_page.wagtail_page_last_published_on as cms_coursepage_last_published_on
from courses
left join cms_courses on courses.course_id = cms_courses.course_id
left join wagtail_page on cms_courses.wagtail_page_id = wagtail_page.wagtail_page_id
left join platform on courses.platform_id = platform.platform_id
left join certificate_page_path on wagtail_page.wagtail_page_path like certificate_page_path.wagtail_page_path || '%'
left join course_topics on courses.course_id = course_topics.course_id
left join course_instructors on courses.course_id = course_instructors.course_id

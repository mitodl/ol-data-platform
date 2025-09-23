with
    courses as (select * from {{ ref("int__ocw__courses") }}),
    instructors as (
        select course_uuid, array_join(array_agg(course_instructor_title), ', ') as course_instructors
        from {{ ref("int__ocw__course_instructors") }}
        group by course_uuid
    )

select
    courses.course_readable_id as course_short_id,
    courses.course_name,
    courses.course_title,
    courses.course_term,
    courses.course_year,
    courses.course_level,
    courses.course_primary_course_number,
    courses.course_extra_course_numbers,
    courses.course_is_live,
    courses.course_is_unpublished,
    courses.course_has_never_published,
    courses.course_live_url,
    courses.course_first_published_on,
    courses.course_publish_date_updated_on,
    courses.course_topics,
    courses.course_learning_resource_types,
    courses.course_department_numbers,
    instructors.course_instructors
from courses
left join instructors on courses.course_uuid = instructors.course_uuid

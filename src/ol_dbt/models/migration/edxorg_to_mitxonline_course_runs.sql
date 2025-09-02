with mitx_courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, mitxonline_course_department as (
    select
        course_id
        , array_join(array_agg(coursedepartment_name), ', ') as department_name
    from {{ ref('int__mitxonline__course_to_departments') }}
    group by course_id
)

, edx_courseruns as (
    select
        *
        , element_at(split(course_number, '.'), 1) as extracted_department_number
    from {{ ref('int__edxorg__mitx_courseruns') }}
)

, edxorg_enrollments as (
    select * from {{ ref('int__edxorg__mitx_courserun_enrollments') }}
)

select distinct
    mitx_courses.mitxonline_course_id
    , mitx_courses.course_readable_id
    , mitx_courses.course_title
    , edx_courseruns.courserun_is_self_paced as is_self_paced
    , edx_courseruns.courserun_is_published as is_published
    , edx_courseruns.courserun_title
    , {{ format_course_id('edx_courseruns.courserun_readable_id', false) }} as courseware_id
    , element_at(split(edx_courseruns.courserun_readable_id, '/'), 3) as run_tag
    , from_iso8601_timestamp(edx_courseruns.courserun_enrollment_start_date) as enrollment_start
    , from_iso8601_timestamp(edx_courseruns.courserun_enrollment_end_date) as enrollment_end
    , from_iso8601_timestamp(edx_courseruns.courserun_start_date) as start_date
    , from_iso8601_timestamp(edx_courseruns.courserun_end_date) as end_date
    , coalesce(
        mitxonline_course_department.department_name
        , edx_courseruns.coursedepartment_name
        , {{ transform_edx_department_number('edx_courseruns.extracted_department_number') }}
    ) as department_name
from mitx_courses
inner join edx_courseruns
    on mitx_courses.course_number = edx_courseruns.course_number
-- ensure we only import course runs that have enrollments on edX.org
inner join edxorg_enrollments
    on edx_courseruns.courserun_readable_id = edxorg_enrollments.courserun_readable_id
left join mitxonline_course_department
    on mitx_courses.mitxonline_course_id = mitxonline_course_department.course_id

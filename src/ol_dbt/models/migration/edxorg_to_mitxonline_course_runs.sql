with
    mitx_courses as (select * from {{ ref("int__mitx__courses") }}),
    mitxonline_courseruns as (select * from {{ ref("int__mitxonline__course_runs") }}),
    mitxonline_course_department as (
        select course_id, array_join(array_agg(coursedepartment_name), ', ') as department_name
        from {{ ref("int__mitxonline__course_to_departments") }}
        group by course_id
    ),
    edx_courseruns as (
        select
            *,
            element_at(split(courserun_readable_id, '/'), 3) as run_tag,
            element_at(split(course_number, '.'), 1) as extracted_department_number,
            {{ format_course_id("courserun_readable_id", false) }} as courseware_id
        from {{ ref("int__edxorg__mitx_courseruns") }}
    ),
    edx_signatories as (
        select courserun_readable_id, array_agg(signatory_normalized_name) as signatory_names
        from {{ ref("stg__edxorg__s3__course_certificate_signatory") }}
        group by courserun_readable_id
    ),
    edx_enrollments as (
        select courserun_readable_id, count(*) as enrollment_count
        from {{ ref("int__edxorg__mitx_courserun_enrollments") }}
        group by courserun_readable_id
    ),
    edx_certificates as (
        select courserun_readable_id, count(*) as certificate_count
        from {{ ref("int__edxorg__mitx_courserun_certificates") }}
        group by courserun_readable_id
    )

select distinct
    mitx_courses.mitxonline_course_id,
    if(
        mitx_courses.mitxonline_course_id is not null,
        mitx_courses.course_readable_id,
        'course-v1:MITx+' || mitx_courses.course_number
    ) as course_readable_id,
    mitx_courses.course_title,
    edx_courseruns.courserun_is_self_paced as is_self_paced,
    edx_courseruns.courserun_is_published as is_published,
    edx_courseruns.courserun_title,
    edx_courseruns.courseware_id,
    edx_courseruns.run_tag,
    from_iso8601_timestamp(edx_courseruns.courserun_enrollment_start_date) as enrollment_start,
    from_iso8601_timestamp(edx_courseruns.courserun_enrollment_end_date) as enrollment_end,
    from_iso8601_timestamp(edx_courseruns.courserun_start_date) as start_date,
    from_iso8601_timestamp(edx_courseruns.courserun_end_date) as end_date,
    coalesce(
        mitxonline_course_department.department_name,
        edx_courseruns.coursedepartment_name,
        {{ transform_edx_department_number("edx_courseruns.extracted_department_number") }}
    ) as department_name,
    edx_signatories.signatory_names,
    edx_enrollments.enrollment_count,
    edx_certificates.certificate_count
from mitx_courses
inner join edx_courseruns on mitx_courses.course_number = edx_courseruns.course_number
inner join edx_enrollments on edx_courseruns.courserun_readable_id = edx_enrollments.courserun_readable_id
left join edx_certificates on edx_courseruns.courserun_readable_id = edx_certificates.courserun_readable_id
left join mitxonline_course_department on mitx_courses.mitxonline_course_id = mitxonline_course_department.course_id
left join
    mitxonline_courseruns
    on edx_courseruns.course_number = mitxonline_courseruns.course_number
    and edx_courseruns.run_tag = mitxonline_courseruns.courserun_tag
left join edx_signatories on edx_courseruns.courseware_id = edx_signatories.courserun_readable_id
where
    from_iso8601_timestamp(edx_courseruns.courserun_end_date) < current_date  -- unenrollable runs
    and mitxonline_courseruns.course_number is null  -- not already in mitxonline course runs

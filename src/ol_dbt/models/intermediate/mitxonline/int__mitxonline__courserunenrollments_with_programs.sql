with enrollments as (
    select *
    from {{ ref('int__mitxonline__courserunenrollments') }}
)

, program_requirements as (
    select * from {{ ref('int__mitx__program_requirements') }}
)

, mitxonline_good_economics_for_hard_times_programs as (
    select * from {{ ref('__mitxonline_good_economics_for_hard_times_program') }}
)

, programs as (
    select * from {{ ref('int__mitx__programs') }}
)

, main_query as (
    select
        enrollments.courserunenrollment_id
        , enrollments.courserunenrollment_platform
        , enrollments.courserunenrollment_is_active
        , enrollments.courserunenrollment_created_on
        , enrollments.courserunenrollment_enrollment_mode
        , enrollments.courserunenrollment_enrollment_status
        , enrollments.courserun_id
        , enrollments.course_id
        , enrollments.courserun_title
        , enrollments.courserun_readable_id
        , enrollments.course_number
        , enrollments.user_id
        , enrollments.user_email
        , enrollments.user_full_name
        , enrollments.user_username
        , enrollments.user_edxorg_username
        , enrollments.user_address_country
        , enrollments.courserunenrollment_is_edx_enrolled
        , program_requirements.micromasters_program_id
        , program_requirements.mitxonline_program_id
        , program_requirements.program_title
        , programs.is_micromasters_program
        , programs.is_dedp_program
    from enrollments
    inner join program_requirements on program_requirements.course_number = enrollments.course_number
    inner join programs
        on programs.mitxonline_program_id = program_requirements.mitxonline_program_id
    where enrollments.course_number != '{{ "dedp_mitxonline_good_economics_for_hard_times_course_number" }}'
)

, good_economics_for_hard_times_query as (
    select
        enrollments.courserunenrollment_id
        , enrollments.courserunenrollment_platform
        , enrollments.courserunenrollment_is_active
        , enrollments.courserunenrollment_created_on
        , enrollments.courserunenrollment_enrollment_mode
        , enrollments.courserunenrollment_enrollment_status
        , enrollments.courserun_id
        , enrollments.course_id
        , enrollments.courserun_title
        , enrollments.courserun_readable_id
        , enrollments.course_number
        , enrollments.user_id
        , enrollments.user_email
        , enrollments.user_full_name
        , enrollments.user_username
        , enrollments.user_edxorg_username
        , enrollments.user_address_country
        , enrollments.courserunenrollment_is_edx_enrolled
        , programs.micromasters_program_id
        , programs.mitxonline_program_id
        , programs.program_title
        , programs.is_micromasters_program
        , programs.is_dedp_program
    from enrollments
    inner join mitxonline_good_economics_for_hard_times_programs
        on mitxonline_good_economics_for_hard_times_programs.courserunenrollment_id = enrollments.courserunenrollment_id
    inner join programs
        on programs.mitxonline_program_id = mitxonline_good_economics_for_hard_times_programs.program_id
    where enrollments.course_number = '{{ "dedp_mitxonline_good_economics_for_hard_times_course_number" }}'
)

select *
from main_query
union all
select *
from good_economics_for_hard_times_query

with
    enrollments as (
        select * from {{ ref("int__mitx__courserun_enrollments") }} where platform = '{{ var("mitxonline") }}'
    ),
    users as (select * from {{ ref("int__mitxonline__users") }})

select
    enrollments.course_number,
    enrollments.courserun_title,
    enrollments.courserun_readable_id,
    enrollments.courserunenrollment_is_active,
    enrollments.courserunenrollment_enrollment_mode,
    enrollments.courserunenrollment_enrollment_status,
    enrollments.courserunenrollment_created_on,
    enrollments.user_mitxonline_username as user_username,
    enrollments.user_email,
    enrollments.user_full_name,
    users.user_address_country,
    users.user_highest_education,
    users.user_gender,
    users.user_birth_year,
    users.user_company,
    users.user_job_title,
    users.user_industry
from enrollments
left join users on enrollments.user_email = users.user_email

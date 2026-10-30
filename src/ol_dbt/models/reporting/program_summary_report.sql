with program_enrollments as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

-- Learners are counted by email, lowercased because the platforms store some addresses
-- in different letter case
, course_enrollments as (
    select
        course_readable_id
        , courserun_readable_id
        , lower(user_email) as user_email
        , user_country_code
        , courserunenrollment_enrollment_mode
        , courseruncertificate_is_earned
    from {{ ref('marts__combined_course_enrollment_detail') }}
)

, program_courses as (
    select * from {{ ref('marts__combined_coursesinprogram') }}
)

-- Both marts carry the program each row counts toward, so the report only groups on it
, program_enrollment_programs as (
    select
        lower(user_email) as user_email
        , user_has_completed_program
        , program_report_key as program_key
        , report_program_readable_id as program_readable_id
        , report_program_title as program_title
        , report_program_name as program_name
        , report_program_type as program_type
    from program_enrollments
)

, program_course_keys as (
    select
        course_readable_id
        , program_report_key as program_key
    from program_courses
)

, aggregated_course_enrollments as (
    select
        program_course_keys.program_key
        , count(distinct {{ dbt.concat(["course_enrollments.courserun_readable_id", "course_enrollments.user_email"]) }}) as total_enrollments
        , count(distinct course_enrollments.user_email) as unique_users
        , count(distinct course_enrollments.user_country_code) as unique_countries
        , count(
            distinct case
                when course_enrollments.courserunenrollment_enrollment_mode = 'verified'
                    then {{ dbt.concat(["course_enrollments.courserun_readable_id", "course_enrollments.user_email"]) }}
            end
        ) as verified_enrollments
        , count(
            distinct case
                when course_enrollments.courserunenrollment_enrollment_mode = 'verified'
                    then course_enrollments.user_email
            end
        ) as unique_verified_users
        , count(
            distinct case
                when course_enrollments.courseruncertificate_is_earned = true
                    then {{ dbt.concat(["course_enrollments.courserun_readable_id", "course_enrollments.user_email"]) }}
            end
        ) as course_certificates
        , count(
            distinct case
                when course_enrollments.courseruncertificate_is_earned = true
                    then course_enrollments.user_email
            end
        ) as unique_course_certificate_earners
    from course_enrollments
    inner join program_course_keys on course_enrollments.course_readable_id = program_course_keys.course_readable_id
    group by program_course_keys.program_key
)

, aggregated_program_enrollements as (
    select
        program_readable_id
        , program_title
        , program_name
        , program_type
        , program_key
        , count(
              distinct case
                  when user_has_completed_program = true
                     then user_email
              end
        ) as program_certificates
    from program_enrollment_programs
    group by program_readable_id, program_key, program_title, program_name, program_type
)

select
    aggregated_program_enrollements.program_readable_id
    , aggregated_program_enrollements.program_title
    , aggregated_program_enrollements.program_name
    , aggregated_program_enrollements.program_type
    , aggregated_course_enrollments.total_enrollments
    , aggregated_course_enrollments.unique_users
    , aggregated_course_enrollments.unique_countries
    , aggregated_course_enrollments.verified_enrollments
    , aggregated_course_enrollments.unique_verified_users
    , aggregated_course_enrollments.course_certificates
    , aggregated_course_enrollments.unique_course_certificate_earners
    , aggregated_program_enrollements.program_certificates
from aggregated_course_enrollments
inner join aggregated_program_enrollements
    on aggregated_course_enrollments.program_key = aggregated_program_enrollements.program_key

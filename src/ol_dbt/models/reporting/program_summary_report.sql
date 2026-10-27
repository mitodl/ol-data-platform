with program_enrollments as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

, course_enrollments as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, program_courses as (
    select * from {{ ref('marts__combined_coursesinprogram') }}
)

, program_title_to_mitxonline_program as (
    select * from {{ ref('int__mitx__program_title_to_mitxonline_program') }}
)

, mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

-- One row per program. An edX.org program that maps to a MITx Online program counts in
-- that program's row. One that maps to none keeps its own row, keyed on its title when
-- it has no readable id.
, program_enrollment_programs as (
    select
        program_enrollments.user_email
        , program_enrollments.user_has_completed_program
        , coalesce(mitxonline_programs.program_readable_id, program_enrollments.program_readable_id)
            as program_readable_id
        , coalesce(mitxonline_programs.program_title, program_enrollments.program_title) as program_title
        , coalesce(mitxonline_programs.program_name, program_enrollments.program_name) as program_name
        , coalesce(mitxonline_programs.program_type, program_enrollments.program_type) as program_type
    from program_enrollments
    left join program_title_to_mitxonline_program
        on
            program_enrollments.platform_name = '{{ var("edxorg") }}'
            and program_enrollments.program_title = program_title_to_mitxonline_program.program_title
    left join mitxonline_programs
        on program_title_to_mitxonline_program.mitxonline_program_id = mitxonline_programs.program_id
)

-- The same key for course membership. MITx Online and xPRO rows carry their program's
-- readable id; the edX.org and MicroMasters rows are mapped by title.
, program_course_keys as (
    select
        program_courses.course_readable_id
        , coalesce(
            program_courses.program_readable_id
            , program_title_to_mitxonline_program.mitxonline_program_readable_id
            , program_courses.program_title
        ) as program_key
    from program_courses
    left join program_title_to_mitxonline_program
        on
            program_courses.program_readable_id is null
            and program_courses.program_title = program_title_to_mitxonline_program.program_title
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
        , coalesce(program_readable_id, program_title) as program_key
        , count(
              distinct case
                  when user_has_completed_program = true
                     then user_email
              end
        ) as program_certificates
    from program_enrollment_programs
    group by program_readable_id, program_title, program_name, program_type
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

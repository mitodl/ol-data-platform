with program_enrollments as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

, course_enrollments as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, program_courses as (
    select * from {{ ref('marts__combined_coursesinprogram') }}
)

, program_report_key as (
    select * from {{ ref('dim_program_report_key') }}
)

-- Program enrollment rows and program course rows, so that one lookup below keys both
, program_rows as (
    select
        'enrollment' as row_source
        , program_enrollments.user_email
        , program_enrollments.user_has_completed_program
        , cast(null as varchar) as course_readable_id
        , program_enrollments.program_readable_id
        , program_enrollments.program_title
    from program_enrollments

    union all

    select
        'course' as row_source
        , cast(null as varchar) as user_email
        , cast(null as boolean) as user_has_completed_program
        , program_courses.course_readable_id
        , program_courses.program_readable_id
        , program_courses.program_title
    from program_courses
)

-- A row whose readable id is a MITx Online or xPRO program's takes that program, including
-- a MicroMasters row the mart gave its MITx Online program's id. Any other row (the mart's
-- generated edX.org ids, and coursesinprogram rows with none) is looked up by its title.
, keyed_program_rows as (
    select
        program_rows.row_source
        , program_rows.user_email
        , program_rows.user_has_completed_program
        , program_rows.course_readable_id
        , coalesce(by_readable_id.program_key, by_title.program_key) as program_key
        , coalesce(by_readable_id.program_readable_id, by_title.program_readable_id) as program_readable_id
        , coalesce(by_readable_id.program_title, by_title.program_title) as program_title
        , coalesce(by_readable_id.program_name, by_title.program_name) as program_name
        , coalesce(by_readable_id.program_type, by_title.program_type) as program_type
    from program_rows
    left join program_report_key as by_readable_id
        on
            by_readable_id.lookup_type = 'readable_id'
            and program_rows.program_readable_id = by_readable_id.lookup_value
    left join program_report_key as by_title
        on
            by_readable_id.lookup_value is null
            and by_title.lookup_type = 'title'
            and program_rows.program_title = by_title.lookup_value
)

, program_enrollment_programs as (
    select * from keyed_program_rows where row_source = 'enrollment'
)

, program_course_keys as (
    select * from keyed_program_rows where row_source = 'course'
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

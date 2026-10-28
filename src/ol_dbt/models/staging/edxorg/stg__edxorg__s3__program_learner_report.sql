with source as (
    select
        *
        -- Each report is a full snapshot of its program, so a key's newest row is the one
        -- in its newest report. The file's mtime does not order the backlog, which was
        -- copied into the landing zone in bulk, so the report date in its name leads.
        , {{ regexp_extract_or_null('_source_file', "'learner_report__(\\d{4}-\\d{2}-\\d{2})\\.csv$'", 1) }} as report_date
        , case
            when date_program_certificate_awarded = 'null' then null -- noqa: ST10
            else
            -- Try parsing once and handle both formats
                coalesce(
                    {{ try_or_null(cast_timestamp_to_iso8601(date_parse('date_program_certificate_awarded', "'%Y-%m-%dT%H:%i:%sZ'", strict=false))) }}
                    , {{ cast_timestamp_to_iso8601(date_parse('date_program_certificate_awarded', "'%Y-%m-%d %H:%i:%s Z'", strict=false)) }}
                )

        end as program_certificate_awarded_at
    from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__program_learner_report') }}
)

{{ deduplicate_raw_table(
    raw_table='raw__edxorg__s3__program_learner_report'
    , partition_columns='user_id, course_run_key, program_uuid'
    , first_by='report_date'
) }}

, aggregated_program_certificate as (
    select
        cast(user_id as integer) as user_id
        , program_uuid
        , course_run_key as courserun_readable_id
        , min(program_certificate_awarded_at) as earliest_program_cert_award_on
        , max(completed_program) as ever_completed_program
    from source
    group by 1, 2, 3
)

, cleaned as (

    select
        authoring_institution as org_id
        , program_type
        , program_uuid
        , username as user_username
        , name as user_full_name
        , course_run_key as courserun_readable_id
        , course_title
        , track as courserunenrollment_enrollment_mode
        , cast(user_id as integer) as user_id
        , cast(completed as boolean) as user_has_completed_course
        , cast(completed_program as boolean) as user_has_completed_program
        , cast(currently_enrolled as boolean) as courserunenrollment_is_active
        , cast(purchased_as_bundle as boolean) as user_has_purchased_as_bundle
        , if(user_roles = 'null', null, user_roles) as user_roles -- noqa: ST10
        , if(letter_grade = 'null', null, letter_grade) as courserungrade_letter_grade -- noqa: ST10
        , if(grade = 'null', null, grade) as courserungrade_grade
        , case
            when program_uuid like '%941d3eaf56966c7' then 'Finance'
            when program_uuid like '%3173ff51e11a748' then 'MIT Finance'
            when program_uuid like '%8c11bfd9c0d7b07' then 'Statistics and Data Science (General Track)'
            when program_uuid like '%cd7c6461dd9b1d4' then 'Statistics and Data Science (Social Sciences Track)'
            else program_title
        end as program_title
        , {{ cast_timestamp_to_iso8601(date_parse('course_run_start_date', "'%Y-%m-%d %H:%i:%s Z'")) }} as courserun_start_on
        , {{ cast_timestamp_to_iso8601(date_parse('date_first_enrolled', "'%Y-%m-%d %H:%i:%s Z'")) }} as courserunenrollment_created_on
        , case
            when date_completed = 'null' then null -- noqa: ST10
            else {{ cast_timestamp_to_iso8601(date_parse('date_completed', "'%Y-%m-%d %H:%i:%s Z'", strict=false)) }}
        end as completed_course_on
        , case
            when last_activity_date = 'null' then null -- noqa: ST10
            else {{ cast_date_to_iso8601('last_activity_date') }}
        end as courseactivity_last_activity_date
        , case
            when date_last_unenrolled = 'null' then null -- noqa: ST10
            else {{ cast_timestamp_to_iso8601(date_parse('date_last_unenrolled', "'%Y-%m-%d %H:%i:%s Z'", strict=false)) }}
        end as courserunenrollment_unenrolled_on
        , case
            when date_first_upgraded_to_verified = 'null' then null -- noqa: ST10
            else {{ cast_timestamp_to_iso8601(date_parse('date_first_upgraded_to_verified', "'%Y-%m-%d %H:%i:%s Z'", strict=false)) }}
        end as courserunenrollment_upgraded_on
        , program_certificate_awarded_at as program_certificate_awarded_on
    from most_recent_source

)

select
    cleaned.org_id
    , cleaned.program_type
    , cleaned.program_uuid
    , cleaned.program_title
    , cleaned.user_username
    , cleaned.user_full_name
    , cleaned.courserun_readable_id
    , cleaned.course_title
    , cleaned.courserunenrollment_enrollment_mode
    , cleaned.user_id
    , cleaned.user_has_completed_course
    , cast(aggregated_program_certificate.ever_completed_program as boolean) as user_has_completed_program
    , cleaned.courserunenrollment_is_active
    , cleaned.user_has_purchased_as_bundle
    , cleaned.user_roles
    , cleaned.courserungrade_letter_grade
    , cleaned.courserungrade_grade
    , cleaned.courserun_start_on
    , cleaned.courserunenrollment_created_on
    , cleaned.completed_course_on
    , cleaned.courseactivity_last_activity_date
    , cleaned.courserunenrollment_unenrolled_on
    , cleaned.courserunenrollment_upgraded_on
    , aggregated_program_certificate.earliest_program_cert_award_on as program_certificate_awarded_on
    , {{ regexp_extract_or_null('cleaned.program_title', "'\((.*?)\)'", 1) }} as program_track
from cleaned
left join aggregated_program_certificate
    on
        cleaned.user_id = aggregated_program_certificate.user_id
        and cleaned.program_uuid = aggregated_program_certificate.program_uuid
        and cleaned.courserun_readable_id = aggregated_program_certificate.courserun_readable_id

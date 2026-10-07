with source as (
    select
        *
        , case
            when {{ adapter.quote('date program certificate awarded') }} = 'null' then null -- noqa: ST10
            else
            -- Try parsing once and handle both formats
                coalesce(
                    {{ try_or_null(cast_timestamp_to_iso8601(date_parse(adapter.quote('date program certificate awarded'), "'%Y-%m-%dT%H:%i:%sZ'"))) }}
                    , {{ cast_timestamp_to_iso8601(date_parse(adapter.quote('date program certificate awarded'), "'%Y-%m-%d %H:%i:%s Z'")) }}
                )

        end as program_certificate_awarded_at
    from {{ source('ol_warehouse_raw_data','raw__edxorg__program_learner_report') }}
)

{{ deduplicate_raw_table(
    raw_table='raw__edxorg__program_learner_report'
    , partition_columns=adapter.quote('user id') ~ ', ' ~ adapter.quote('course run key') ~ ', ' ~ adapter.quote('program uuid')
) }}

, aggregated_program_certificate as (
    select
        cast({{ adapter.quote('user id') }} as integer) as user_id
        , {{ adapter.quote('program uuid') }} as program_uuid
        , {{ adapter.quote('course run key') }} as courserun_readable_id
        , min(program_certificate_awarded_at) as earliest_program_cert_award_on
        , max({{ adapter.quote('completed program') }}) as ever_completed_program
    from source
    group by 1, 2, 3
)

, cleaned as (

    select
        {{ adapter.quote('authoring institution') }} as org_id
        , {{ adapter.quote('program type') }} as program_type
        , {{ adapter.quote('program uuid') }} as program_uuid
        , username as user_username
        , name as user_full_name
        , {{ adapter.quote('course run key') }} as courserun_readable_id
        , {{ adapter.quote('course title') }} as course_title
        , track as courserunenrollment_enrollment_mode
        , cast({{ adapter.quote('user id') }} as integer) as user_id
        , cast(completed as boolean) as user_has_completed_course
        , cast({{ adapter.quote('completed program') }} as boolean) as user_has_completed_program
        , cast({{ adapter.quote('currently enrolled') }} as boolean) as courserunenrollment_is_active
        , cast({{ adapter.quote('purchased as bundle') }} as boolean) as user_has_purchased_as_bundle
        , if({{ adapter.quote('user roles') }} = 'null', null, {{ adapter.quote('user roles') }}) as user_roles -- noqa: ST10
        , if({{ adapter.quote('letter grade') }} = 'null', null, {{ adapter.quote('letter grade') }}) as courserungrade_letter_grade -- noqa: ST10
        , if(grade = 'null', null, grade) as courserungrade_grade
        , case
            when {{ adapter.quote('program uuid') }} like '%941d3eaf56966c7' then 'Finance'
            when {{ adapter.quote('program uuid') }} like '%3173ff51e11a748' then 'MIT Finance'
            when {{ adapter.quote('program uuid') }} like '%8c11bfd9c0d7b07' then 'Statistics and Data Science (General Track)'
            when {{ adapter.quote('program uuid') }} like '%cd7c6461dd9b1d4' then 'Statistics and Data Science (Social Sciences Track)'
            else {{ adapter.quote('program title') }}
        end as program_title
        , {{ cast_timestamp_to_iso8601(date_parse(adapter.quote('course run start date'), "'%Y-%m-%d %H:%i:%s Z'")) }} as courserun_start_on
        , {{ cast_timestamp_to_iso8601(date_parse(adapter.quote('date first enrolled'), "'%Y-%m-%d %H:%i:%s Z'")) }} as courserunenrollment_created_on
        , case
            when {{ adapter.quote('date completed') }} = 'null' then null -- noqa: ST10
            else {{ cast_timestamp_to_iso8601(date_parse(adapter.quote('date completed'), "'%Y-%m-%d %H:%i:%s Z'")) }}
        end as completed_course_on
        , case
            when {{ adapter.quote('last activity date') }} = 'null' then null -- noqa: ST10
            else {{ cast_date_to_iso8601(adapter.quote('last activity date')) }}
        end as courseactivity_last_activity_date
        , case
            when {{ adapter.quote('date last unenrolled') }} = 'null' then null -- noqa: ST10
            else {{ cast_timestamp_to_iso8601(date_parse(adapter.quote('date last unenrolled'), "'%Y-%m-%d %H:%i:%s Z'")) }}
        end as courserunenrollment_unenrolled_on
        , case
            when {{ adapter.quote('date first upgraded to verified') }} = 'null' then null -- noqa: ST10
            else {{ cast_timestamp_to_iso8601(date_parse(adapter.quote('date first upgraded to verified'), "'%Y-%m-%d %H:%i:%s Z'")) }}
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

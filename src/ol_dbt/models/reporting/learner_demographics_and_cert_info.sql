with combined__users as (
    select * from {{ ref('marts__combined__users') }}
)

, program_enrollment_detail as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

, combined_course_enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, combined_coursesinprogram as (
    select * from {{ ref('marts__combined_coursesinprogram') }}
)

-- Both program marts carry the program each row counts toward, keyed as in
-- program_summary_report. Learners are matched by email, lowercased because the
-- platforms store some addresses in different letter case.
, program_enrollments as (
    select
        lower(user_email) as user_email
        , program_report_key as program_key
        , report_program_readable_id as program_readable_id
        , report_program_title as program_title
        , max(case when user_has_completed_program = true then 1 else 0 end) as has_completed_program
    from program_enrollment_detail
    where program_report_key is not null
    group by lower(user_email), program_report_key, report_program_readable_id, report_program_title
)

, program_course_keys as (
    select distinct
        course_readable_id
        , program_report_key as program_key
        , report_program_readable_id as program_readable_id
        , report_program_title as program_title
    from combined_coursesinprogram
    where program_report_key is not null
)

, learner_program_courses as (
    select distinct
        lower(combined_course_enrollment_detail.user_email) as user_email
        , combined_course_enrollment_detail.course_readable_id
        , program_course_keys.program_key
        , program_course_keys.program_readable_id
        , program_course_keys.program_title
    from combined_course_enrollment_detail
    inner join program_course_keys
        on combined_course_enrollment_detail.course_readable_id = program_course_keys.course_readable_id
    where combined_course_enrollment_detail.courseruncertificate_is_earned = true
)

, program_course_certificates as (
    select
        user_email
        , program_key
        , program_readable_id
        , program_title
        , count(distinct course_readable_id) as program_course_cert_count
    from learner_program_courses
    group by user_email, program_key, program_readable_id, program_title
)

-- The program name each program key reports under, from the program enrollment mart.
-- Programs that share a name (e.g. the three DEDP programs) are counted together below.
-- A program with no enrollment rows has no name here and counts on its own key.
, program_names as (
    select
        program_report_key as program_key
        , min(report_program_name) as program_name
    from program_enrollment_detail
    where program_report_key is not null
    group by program_report_key
)

, program_name_course_certificates as (
    select
        learner_program_courses.user_email
        , coalesce(program_names.program_name, learner_program_courses.program_key) as program_name_group
        , count(distinct learner_program_courses.course_readable_id) as program_name_course_cert_count
    from learner_program_courses
    left join program_names
        on learner_program_courses.program_key = program_names.program_key
    group by
        learner_program_courses.user_email
        , coalesce(program_names.program_name, learner_program_courses.program_key)
)

, user_programs as (
    select
        user_email
        , program_key
    from program_enrollments

    union distinct

    select
        user_email
        , program_key
    from program_course_certificates
)

select
    combined__users.user_hashed_id
    , combined__users.platforms
    , combined__users.user_full_name
    , combined__users.user_birth_year
    , combined__users.user_gender
    , combined__users.user_email
    , combined__users.user_edxorg_username
    , combined__users.user_mitxonline_username
    , combined__users.user_address_country
    , combined__users.user_highest_education
    , combined__users.user_job_title
    , combined__users.user_company
    , combined__users.user_industry
    , combined__users.latest_income_usd
    , combined__users.first_course_start_datetime
    , combined__users.last_course_start_datetime
    , combined__users.num_of_course_enrolled
    , coalesce(program_enrollments.program_readable_id, program_course_certificates.program_readable_id)
        as program_readable_id
    , coalesce(program_enrollments.program_title, program_course_certificates.program_title) as program_title
    , program_course_certificates.program_course_cert_count
    , program_name_course_certificates.program_name_course_cert_count
    , case when program_enrollments.has_completed_program = 1 then 'Y' end as program_cert_ind
    , case when program_enrollments.user_email is not null then 'Y' end as program_enroll_ind
from combined__users
left join user_programs
    on lower(combined__users.user_email) = user_programs.user_email
left join program_enrollments
    on
        user_programs.user_email = program_enrollments.user_email
        and user_programs.program_key = program_enrollments.program_key
left join program_course_certificates
    on
        user_programs.user_email = program_course_certificates.user_email
        and user_programs.program_key = program_course_certificates.program_key
left join program_names
    on user_programs.program_key = program_names.program_key
left join program_name_course_certificates
    on
        user_programs.user_email = program_name_course_certificates.user_email
        and coalesce(program_names.program_name, user_programs.program_key)
        = program_name_course_certificates.program_name_group
where combined__users.user_hashed_id is not null

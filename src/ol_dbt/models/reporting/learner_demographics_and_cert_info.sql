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

, program_enrollments as (
    select
        user_email
        , program_name
        , max(case when user_has_completed_program = true then 1 else 0 end) as has_completed_program
    from program_enrollment_detail
    group by user_email, program_name
)

, completed_program_tracks as (
    select distinct
        user_email
        , program_name
        , program_track
    from program_enrollment_detail
    where
        user_has_completed_program = true
        and program_track is not null
)

, completed_tracks_by_program as (
    select
        user_email
        , program_name
        , {{ array_join('array_agg(program_track order by program_track)', ', ') }} as program_completed_tracks
    from completed_program_tracks
    group by user_email, program_name
)

, program_course_certificates as (
    select
        combined_course_enrollment_detail.user_email
        , combined_coursesinprogram.program_name
        , count(distinct combined_course_enrollment_detail.course_readable_id) as program_course_cert_count
    from combined_course_enrollment_detail
    inner join combined_coursesinprogram
        on combined_course_enrollment_detail.course_readable_id = combined_coursesinprogram.course_readable_id
    where combined_course_enrollment_detail.courseruncertificate_is_earned = true
    group by combined_course_enrollment_detail.user_email, combined_coursesinprogram.program_name
)

, user_programs as (
    select
        user_email
        , program_name
    from program_enrollments

    union distinct

    select
        user_email
        , program_name
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
    , user_programs.program_name
    , program_course_certificates.program_course_cert_count
    , case when program_enrollments.has_completed_program = 1 then 'Y' end as program_cert_ind
    , completed_tracks_by_program.program_completed_tracks
    , case when program_enrollments.user_email is not null then 'Y' end as program_enroll_ind
from combined__users
left join user_programs
    on combined__users.user_email = user_programs.user_email
left join program_enrollments
    on
        user_programs.user_email = program_enrollments.user_email
        and user_programs.program_name = program_enrollments.program_name
left join completed_tracks_by_program
    on
        user_programs.user_email = completed_tracks_by_program.user_email
        and user_programs.program_name = completed_tracks_by_program.program_name
left join program_course_certificates
    on
        user_programs.user_email = program_course_certificates.user_email
        and user_programs.program_name = program_course_certificates.program_name
where combined__users.user_hashed_id is not null

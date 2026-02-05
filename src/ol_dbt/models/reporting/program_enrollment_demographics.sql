with program_enrollments as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

, users as (
    select * from {{ ref('marts__combined__users') }}
)

, courses_in_programs as (
    select * from {{ ref('marts__combined_coursesinprogram') }}
)

, combined_enrollments as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, program_course_stats as (
    select
        pe.platform_name
        , pe.program_name
        , pe.user_email
        , count(distinct cip.course_readable_id) as courses_taken_in_program
        , sum(case when upper(ce.course_title) like '%CAPSTONE%' then 1 else 0 end) as capstone_sum
        , min(cast(substring(ce.courserun_start_on, 1, 10) as date)) as first_courserun_start_on_date
    from program_enrollments pe
    left join courses_in_programs cip
        on pe.platform_name = cip.platform
        and pe.program_name = cip.program_name
        and pe.program_id = cip.program_id
    left join combined_enrollments ce
        on pe.user_email = ce.user_email
        and cip.course_readable_id = ce.course_readable_id
        and ce.courserunenrollment_enrollment_mode = 'verified'
    group by
        pe.platform_name
        , pe.program_name
        , pe.user_email
)

select
    pe.platform_name
    , pe.program_id
    , pe.program_title
    , pe.program_name
    , pe.program_track
    , pe.program_type
    , pe.program_is_live
    , pe.program_readable_id
    , pe.user_hashed_id
    , pe.user_id
    , pe.user_email
    , pe.user_username
    , pe.user_full_name
    , pe.user_has_completed_program
    , pe.programenrollment_is_active
    , pe.programenrollment_created_on
    , pe.programenrollment_enrollment_status
    , pe.programcertificate_created_on
    , pe.programcertificate_is_revoked
    , pe.programcertificate_uuid
    , pe.programcertificate_url
    , pe.unique_courses_taken_in_program
    , pe.capstone_indicator
    , pe.program_completion_days
    , u.user_highest_education
    , u.user_industry
    , u.user_company
    , u.user_job_title
    , u.user_birth_year
    , u.user_gender
    , u.user_address_country
    , u.user_street_address
    , u.user_address_city
    , u.user_address_state_or_territory
    , u.user_address_postal_code
    , pcs.courses_taken_in_program
    , case when pcs.capstone_sum > 0 then 'Y' else 'N' end as capstone_ind
    , date_diff(
        'day'
        , pcs.first_courserun_start_on_date
        , cast(substring(pe.programcertificate_created_on, 1, 10) as date)
    ) as program_complete_days
from program_enrollments pe
left join users u
    on pe.user_hashed_id = u.user_hashed_id
left join program_course_stats pcs
    on pe.platform_name = pcs.platform_name
    and pe.program_name = pcs.program_name
    and pe.user_email = pcs.user_email

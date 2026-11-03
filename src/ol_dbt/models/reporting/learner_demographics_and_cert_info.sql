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

, program as (
    select * from {{ ref('dim_program') }}
)

, bridge_program_course as (
    select * from {{ ref('bridge_program_course') }}
)

, course_run as (
    select * from {{ ref('dim_course_run') }}
)

, tfact_enrollment as (
    select * from {{ ref('tfact_enrollment') }}
)

, dim_user as (
    select * from {{ ref('dim_user') }}
)

, dedp_indicators as (
    select
        user_email
        , 1 as Any_DEDP_Program_Indicator
        , count(distinct case
            when program_title =
            'Data, Economics, and Design of Policy: International Development'
            then user_email else null end) as DEDP_International_Program_Indicator
        , count (distinct case
            when program_title =
            'Data, Economics, and Design of Policy: Public Policy'
            then user_email else null end) as DEDP_Public_Policy_Program_Indicator
    from program_enrollment_detail
    where program_title in
        ('Data, Economics, and Design of Policy'
        , 'Data, Economics, and Design of Policy: International Development'
        , 'Data, Economics, and Design of Policy: Public Policy')
        and programcertificate_uuid is not null
        and (programcertificate_is_revoked = false or programcertificate_is_revoked is null)
    group by user_email
)

, dedp_enroll_indicators as (
    select
        dim_user.email as user_email
        , 1 as Any_DEDP_Course_Enrollment_Indicator
    from program
    join bridge_program_course
        on program.program_pk = bridge_program_course.program_fk
    join course_run
        on
            bridge_program_course.course_fk = course_run.course_fk
            and course_run.is_current = true
    join tfact_enrollment
        on course_run.courserun_pk = tfact_enrollment.courserun_fk
    join dim_user
        on tfact_enrollment.user_fk = dim_user.user_pk
    where program.program_is_dedp = true
    group by dim_user.email
)

, dedp_certs as (
    select
        combined_course_enrollment_detail.user_email
        , count(distinct combined_course_enrollment_detail.course_readable_id) as DEDP_Course_Cert_Count
    from combined_course_enrollment_detail
    inner join combined_coursesinprogram
        on combined_course_enrollment_detail.course_readable_id = combined_coursesinprogram.course_readable_id
    where combined_coursesinprogram.program_title in
        ('Data, Economics, and Design of Policy'
        , 'Data, Economics, and Design of Policy: International Development'
        , 'Data, Economics, and Design of Policy: Public Policy')
        and combined_course_enrollment_detail.courseruncertificate_is_earned = true
    group by combined_course_enrollment_detail.user_email
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
    , dedp_certs.dedp_course_cert_count
    , case when dedp_indicators.Any_DEDP_Program_Indicator= 1 then 'Y' else null end as any_dedp_program_cert_ind
    , case when dedp_indicators.DEDP_International_Program_Indicator= 1 then 'Y' else null end as dedp_international_program_cert_ind
    , case when dedp_indicators.DEDP_Public_Policy_Program_Indicator= 1 then 'Y' else null end as dedp_public_policy_program_cert_ind
    , case when dedp_enroll_indicators.Any_DEDP_Course_Enrollment_Indicator= 1 then 'Y' else null end as any_dedp_course_enroll_ind
from combined__users
left join dedp_indicators
    on combined__users.user_email = dedp_indicators.user_email
left join dedp_certs
    on combined__users.user_email = dedp_certs.user_email
left join dedp_enroll_indicators
    on combined__users.user_email = dedp_enroll_indicators.user_email
where combined__users.user_hashed_id is not null

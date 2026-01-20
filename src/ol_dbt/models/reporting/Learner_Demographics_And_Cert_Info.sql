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

, dedp_indicators as (
    select
        user_hashed_id
        , 1 as Any_DEDP_Program_Indicator
        , count(distinct case
            when program_title =
            'Data, Economics, and Design of Policy: International Development'
            then user_hashed_id else null end) as DEDP_International_Program_Indicator
        , count (distinct case
            when program_title =
            'Data, Economics, and Design of Policy: Public Policy'
            then user_hashed_id else null end) as DEDP_Public_Policy_Program_Indicator
    from program_enrollment_detail
    where program_title in
        ('Data, Economics, and Design of Policy'
        , 'Data, Economics, and Design of Policy: International Development'
        , 'Data, Economics, and Design of Policy: Public Policy')
        and programcertificate_uuid is not null
        and (programcertificate_is_revoked = false or programcertificate_is_revoked is null)
    group by user_hashed_id
)

, dedp_certs as (
    select
        combined_course_enrollment_detail.user_hashed_id
        , count(distinct combined_course_enrollment_detail.course_readable_id) as DEDP_Course_Cert_Count
    from combined_course_enrollment_detail
    inner join combined_coursesinprogram
        on combined_course_enrollment_detail.course_readable_id = combined_coursesinprogram.course_readable_id
    where combined_coursesinprogram.program_title in
        ('Data, Economics, and Design of Policy'
        , 'Data, Economics, and Design of Policy: International Development'
        , 'Data, Economics, and Design of Policy: Public Policy')
        and combined_course_enrollment_detail.courseruncertificate_is_earned = true
    group by combined_course_enrollment_detail.user_hashed_id
)

select
    combined__users.user_hashed_id
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
from combined__users
left join dedp_indicators
    on combined__users.user_hashed_id = dedp_indicators.user_hashed_id
left join dedp_certs
    on combined__users.user_hashed_id = dedp_certs.user_hashed_id
where combined__users.user_hashed_id is not null

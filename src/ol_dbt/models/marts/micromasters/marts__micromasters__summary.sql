with micromasters_enrollments as (
    select *
    from {{ ref('int__micromasters__course_enrollments') }}
)

, micromasters_course_certificates as (
    select *
    from {{ ref('int__micromasters__course_certificates') }}
)

, micromasters_program_certificates as (
    select *
    from {{ ref('int__micromasters__program_certificates') }}
)

, mitxonline_course_certificates as (
    select *
    from {{ ref('int__mitxonline__courserun_certificates') }}
)

, mitxonline_program_certificates as (
    select *
    from {{ ref('int__mitxonline__program_certificates') }}
)

, enrollments as (
    select    
        case when 
            micromasters_enrollments.program_title in (
                'Data, Economics, and Design of Policy'
                , 'Data, Economics, and Design of Policy: International Development'
                , 'Data, Economics, and Design of Policy: Public Policy'
            )
            then 'Data, Economics, and Design of Policy' 
        when 
            micromasters_enrollments.program_title in (
                'Statistics and Data Science'
                , 'Statistics and Data Science (General track)'
            ) 
            then 'Statistics and Data Science'
        when 
            micromasters_enrollments.program_title in (
                'MIT Finance'
            )
            then 'Finance'
        else micromasters_enrollments.program_title end as program_title
        , count(distinct micromasters_enrollments.courserun_readable_id || user_email) as total_enrollments
        , count(distinct micromasters_enrollments.user_email) as unique_users
        , count(distinct micromasters_enrollments.user_address_country) as unique_countries
        , count(distinct case 
            when micromasters_enrollments.courserunenrollment_enrollment_mode = 'verified'
                then (micromasters_enrollments.courserun_readable_id || micromasters_enrollments.user_email) 
        end) as verified_enrollments
        , count(distinct case 
            when micromasters_enrollments.courserunenrollment_enrollment_mode = 'verified' 
                then micromasters_enrollments.user_email 
        end) as unique_verified_users
    from micromasters_enrollments
    group by 1    
)

, course_certs as (    
    select
        case when 
            micromasters_course_certificates.program_title in (
                'Data, Economics, and Design of Policy'
                , 'Data, Economics, and Design of Policy: International Development'
                , 'Data, Economics, and Design of Policy: Public Policy'
            )
            then 'Data, Economics, and Design of Policy' 
        when 
            micromasters_course_certificates.program_title in (
                'Statistics and Data Science'
                , 'Statistics and Data Science (General track)'
            ) 
            then 'Statistics and Data Science'
        when 
            micromasters_course_certificates.program_title in (
                'MIT Finance'
            )
            then 'Finance'
        else micromasters_course_certificates.program_title end as program_title
        , count(
            distinct
            micromasters_course_certificates.courserun_readable_id 
            || micromasters_course_certificates.user_email
        ) as course_certificates
        , count(distinct micromasters_course_certificates.user_email) as unique_course_certificate_earners
    from micromasters_course_certificates
    left join mitxonline_course_certificates
        on 
            micromasters_course_certificates.courserun_readable_id 
            = mitxonline_course_certificates.courserun_readable_id
            and micromasters_course_certificates.user_mitxonline_username = mitxonline_course_certificates.user_username
    where 
        mitxonline_course_certificates.courseruncertificate_is_revoked = false 
        or mitxonline_course_certificates.courseruncertificate_is_revoked is null
    group by 1
)

, program_certs as (
    select
        case when 
            micromasters_program_certificates.program_title in (
                'Data, Economics, and Design of Policy'
                , 'Data, Economics, and Design of Policy: International Development'
                , 'Data, Economics, and Design of Policy: Public Policy'
            )
            then 'Data, Economics, and Design of Policy' 
        when 
            micromasters_program_certificates.program_title in (
                'Statistics and Data Science'
                , 'Statistics and Data Science (General track)'
            ) 
            then 'Statistics and Data Science'
        when 
            micromasters_program_certificates.program_title in (
                'MIT Finance'
            )
            then 'Finance'
        else micromasters_program_certificates.program_title end as program_title
        , count(distinct micromasters_program_certificates.user_email) as program_certificates
    from micromasters_program_certificates
    left join mitxonline_program_certificates
        on 
            micromasters_program_certificates.mitxonline_program_id = mitxonline_program_certificates.program_id
            and micromasters_program_certificates.user_mitxonline_username 
            = mitxonline_program_certificates.user_username
    where 
        mitxonline_program_certificates.programcertificate_is_revoked = false 
        or mitxonline_program_certificates.programcertificate_is_revoked is null
    group by 1
)

select
    enrollments.program_title
    , enrollments.total_enrollments
    , enrollments.unique_users
    , enrollments.unique_countries
    , enrollments.verified_enrollments
    , enrollments.unique_verified_users
    , course_certs.course_certificates
    , course_certs.unique_course_certificate_earners
    , program_certs.program_certificates
from enrollments
left join course_certs
    on enrollments.program_title = course_certs.program_title
left join program_certs
    on enrollments.program_title = program_certs.program_title
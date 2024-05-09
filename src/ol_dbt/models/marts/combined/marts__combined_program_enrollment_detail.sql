with mitxpro__programenrollments as (
    select * from {{ ref('int__mitxpro__programenrollments') }}
)

, mitxpro__programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

, mitxpro__program_certificates as (
    select * from {{ ref('int__mitxpro__program_certificates') }}
)

, mitxonline__programenrollments as (
    select * from {{ ref('int__mitxonline__programenrollments') }}
)

, mitxonline__programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, mitxonline__program_certificates as (
    select * from {{ ref('int__mitxonline__program_certificates') }}
)

, edx_program_certificates as (
    select * from {{ ref('int__edxorg__mitx_program_certificates') }}
)

, edx_program_enrollments as (
    select * from {{ ref('int__edxorg__mitx_program_enrollments') }}
)

, mitx__users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters__program_enrollments as (
    select * from {{ ref('int__micromasters__program_enrollments') }}
)

, micromasters__users as (
    select * from {{ ref('stg__micromasters__app__postgres__auth_user') }}
)

, combined_programs as (
    select
        mitxpro__programs.platform_name
        , mitxpro__programs.program_id
        , mitxpro__programs.program_title
        , mitxpro__programs.program_is_live
        , mitxpro__programs.program_readable_id
        , mitxpro__programenrollments.user_id
        , mitxpro__programenrollments.user_email
        , mitxpro__programenrollments.user_username
        , mitxpro__programenrollments.programenrollment_is_active
        , mitxpro__programenrollments.programenrollment_created_on
        , mitxpro__programenrollments.programenrollment_enrollment_status
        , mitxpro__program_certificates.programcertificate_created_on
        , mitxpro__program_certificates.programcertificate_is_revoked
        , mitxpro__program_certificates.programcertificate_uuid
    from mitxpro__programenrollments
    inner join mitxpro__programs
        on mitxpro__programenrollments.program_id = mitxpro__programs.program_id
    left join mitxpro__program_certificates
        on
            mitxpro__programenrollments.program_id = mitxpro__program_certificates.program_id
            and mitxpro__programenrollments.user_id = mitxpro__program_certificates.user_id

    union all

    select
        'mitxonline' as platform_name
        , mitxonline__programs.program_id
        , mitxonline__programs.program_title
        , mitxonline__programs.program_is_live
        , mitxonline__programs.program_readable_id
        , mitxonline__programenrollments.user_id
        , mitxonline__programenrollments.user_email
        , mitxonline__programenrollments.user_username
        , mitxonline__programenrollments.programenrollment_is_active
        , mitxonline__programenrollments.programenrollment_created_on
        , mitxonline__programenrollments.programenrollment_enrollment_status
        , mitxonline__program_certificates.programcertificate_created_on
        , mitxonline__program_certificates.programcertificate_is_revoked
        , mitxonline__program_certificates.programcertificate_uuid
    from mitxonline__programenrollments
    inner join mitxonline__programs
        on mitxonline__programenrollments.program_id = mitxonline__programs.program_id
    left join mitxonline__program_certificates
        on
            mitxonline__programenrollments.user_id = mitxonline__program_certificates.user_id
            and mitxonline__programenrollments.program_id = mitxonline__program_certificates.program_id

    union all

    select
        'edxorg' as platform_name
        , edx_program_enrollments.micromasters_program_id as program_id
        , edx_program_enrollments.program_title
        , null as program_is_live
        , null as program_readable_id
        , edx_program_enrollments.user_id
        , mitx__users.user_email
        , edx_program_enrollments.user_username
        , null as programenrollment_is_active
        , null as programenrollment_created_on
        , null as programenrollment_enrollment_status
        , edx_program_certificates.program_certificate_awarded_on as programcertificate_created_on
        , null as programcertificate_is_revoked
        , edx_program_certificates.program_certificate_hashed_id as programcertificate_uuid
    from edx_program_enrollments
    left join mitx__users
        on edx_program_enrollments.user_id = mitx__users.user_id
    left join edx_program_certificates
        on
            edx_program_enrollments.program_uuid = edx_program_certificates.program_uuid
            and edx_program_enrollments.user_id = edx_program_certificates.user_id
)

, mm_no_dups as (
    select 
        micromasters__program_enrollments.platform_name
        , micromasters__program_enrollments.micromasters_program_id as program_id
        , micromasters__program_enrollments.program_title
        , null as program_is_live
        , null as program_readable_id
        , micromasters__users.user_id
        , micromasters__program_enrollments.user_email
        , micromasters__users.user_username
        , null as programenrollment_is_active
        , null as programenrollment_created_on
        , null as programenrollment_enrollment_status
        , null as programcertificate_created_on
        , null as programcertificate_is_revoked
        , null as programcertificate_uuid
    from micromasters__program_enrollments
    inner join micromasters__users
        on micromasters__program_enrollments.micromasters_user_id = micromasters__users.user_id
    left join combined_programs
        on 
            micromasters__program_enrollments.user_email = combined_programs.user_email
            and micromasters__program_enrollments.program_title = combined_programs.program_title
    where 
        combined_programs.user_email is null
        and combined_programs.program_title is null
        and micromasters__program_enrollments.platform_name = 'micromasters'
)

select 
    combined_programs.platform_name
    , combined_programs.program_id
    , combined_programs.program_title
    , combined_programs.program_is_live
    , combined_programs.program_readable_id
    , combined_programs.user_id
    , combined_programs.user_email
    , combined_programs.user_username
    , combined_programs.programenrollment_is_active
    , combined_programs.programenrollment_created_on
    , combined_programs.programenrollment_enrollment_status
    , combined_programs.programcertificate_created_on
    , combined_programs.programcertificate_is_revoked
    , combined_programs.programcertificate_uuid
from combined_programs

union all

select 
    mm_no_dups.platform_name
    , mm_no_dups.program_id
    , mm_no_dups.program_title
    , mm_no_dups.program_is_live
    , mm_no_dups.program_readable_id
    , mm_no_dups.user_id
    , mm_no_dups.user_email
    , mm_no_dups.user_username
    , mm_no_dups.programenrollment_is_active
    , mm_no_dups.programenrollment_created_on
    , mm_no_dups.programenrollment_enrollment_status
    , mm_no_dups.programcertificate_created_on
    , mm_no_dups.programcertificate_is_revoked
    , mm_no_dups.programcertificate_uuid
from mm_no_dups

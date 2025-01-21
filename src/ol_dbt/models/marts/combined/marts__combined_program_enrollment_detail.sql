with mitxpro__programenrollments as (
    select * from {{ ref('int__mitxpro__programenrollments') }}
)

, mitxpro__programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

, mitxpro__program_certificates as (
    select * from {{ ref('int__mitxpro__program_certificates') }}
)

, mitxpro__users as (
    select * from {{ ref('int__mitxpro__users') }}
)

, mitxonline__programenrollments as (
    select * from {{ ref('int__mitxonline__programenrollments') }}
)

, mitxonline__programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, mitxonline__users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, mitx__programs as (
    select * from {{ ref('int__mitx__programs') }}
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

, edxorg__users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters__program_enrollments as (
    select * from {{ ref('int__micromasters__program_enrollments') }}
)

, combined_programs as (
    select
        mitxpro__programs.platform_name
        , mitxpro__programs.program_id
        , mitxpro__programs.program_title
        , mitxpro__programs.program_title as program_name
        , null as program_track
        , 'Professional Certificate' as program_type
        , mitxpro__programs.program_is_live
        , mitxpro__programs.program_readable_id
        , mitxpro__programenrollments.user_id
        , mitxpro__programenrollments.user_email
        , mitxpro__programenrollments.user_username
        , mitxpro__users.user_full_name
        , mitxpro__programenrollments.programenrollment_is_active
        , mitxpro__programenrollments.programenrollment_created_on
        , mitxpro__programenrollments.programenrollment_enrollment_status
        , mitxpro__program_certificates.programcertificate_created_on
        , mitxpro__program_certificates.programcertificate_is_revoked
        , mitxpro__program_certificates.programcertificate_uuid
        , mitxpro__program_certificates.programcertificate_url
        , if(mitxpro__program_certificates.programcertificate_url is not null, true, false)
        as user_has_completed_program
    from mitxpro__programenrollments
    inner join mitxpro__programs
        on mitxpro__programenrollments.program_id = mitxpro__programs.program_id
    left join mitxpro__users
        on mitxpro__programenrollments.user_id = mitxpro__users.user_id
    left join mitxpro__program_certificates
        on
            mitxpro__programenrollments.program_id = mitxpro__program_certificates.program_id
            and mitxpro__programenrollments.user_id = mitxpro__program_certificates.user_id

    union all

    select
        '{{ var("mitxonline") }}' as platform_name
        , mitxonline__programs.program_id
        , mitxonline__programs.program_title
        , mitxonline__programs.program_name
        , mitxonline__programs.program_track
        , mitxonline__programs.program_type
        , mitxonline__programs.program_is_live
        , mitxonline__programs.program_readable_id
        , mitxonline__programenrollments.user_id
        , mitxonline__programenrollments.user_email
        , mitxonline__programenrollments.user_username
        , mitxonline__users.user_full_name
        , mitxonline__programenrollments.programenrollment_is_active
        , mitxonline__programenrollments.programenrollment_created_on
        , mitxonline__programenrollments.programenrollment_enrollment_status
        , mitxonline__program_certificates.programcertificate_created_on
        , mitxonline__program_certificates.programcertificate_is_revoked
        , mitxonline__program_certificates.programcertificate_uuid
        , mitxonline__program_certificates.programcertificate_url
        , if(mitxonline__program_certificates.programcertificate_url is not null, true, false)
        as user_has_completed_program
    from mitxonline__programenrollments
    inner join mitxonline__programs
        on mitxonline__programenrollments.program_id = mitxonline__programs.program_id
    left join mitxonline__users
        on mitxonline__programenrollments.user_id = mitxonline__users.user_id
    left join mitxonline__program_certificates
        on
            mitxonline__programenrollments.user_id = mitxonline__program_certificates.user_id
            and mitxonline__programenrollments.program_id = mitxonline__program_certificates.program_id

    union all

    select
        '{{ var("edxorg") }}' as platform_name
        , edx_program_enrollments.micromasters_program_id as program_id
        , edx_program_enrollments.program_title
        , edx_program_enrollments.program_name
        , edx_program_enrollments.program_track
        , edx_program_enrollments.program_type
        , null as program_is_live
        , null as program_readable_id
        , edx_program_enrollments.user_id
        , edxorg__users.user_email
        , edx_program_enrollments.user_username
        , edxorg__users.user_full_name
        , null as programenrollment_is_active
        , null as programenrollment_created_on
        , null as programenrollment_enrollment_status
        , edx_program_certificates.program_certificate_awarded_on as programcertificate_created_on
        , null as programcertificate_is_revoked
        , edx_program_certificates.program_certificate_hashed_id as programcertificate_uuid
        , null as programcertificate_url
        , if(edx_program_certificates.user_has_completed_program = true, true, false) as user_has_completed_program
    from edx_program_enrollments
    left join edxorg__users
        on edx_program_enrollments.user_id = edxorg__users.user_id
    left join edx_program_certificates
        on
            edx_program_enrollments.program_uuid = edx_program_certificates.program_uuid
            and edx_program_enrollments.user_id = edx_program_certificates.user_id

    union all

    select
        micromasters__program_enrollments.platform_name
        , micromasters__program_enrollments.micromasters_program_id as program_id
        , micromasters__program_enrollments.program_title
        , mitxonline__programs.program_name
        , mitxonline__programs.program_track
        , mitxonline__programs.program_type
        , mitxonline__programs.program_is_live
        , mitxonline__programs.program_readable_id
        , micromasters__program_enrollments.user_edxorg_id as user_id
        , micromasters__program_enrollments.user_email
        , micromasters__program_enrollments.user_edxorg_username as user_username
        , micromasters__program_enrollments.user_full_name
        , null as programenrollment_is_active
        , null as programenrollment_created_on
        , null as programenrollment_enrollment_status
        , edx_program_certificates.program_certificate_awarded_on as programcertificate_created_on
        , null as programcertificate_is_revoked
        , edx_program_certificates.program_certificate_hashed_id as programcertificate_uuid
        , null as programcertificate_url
        , if(edx_program_certificates.user_has_completed_program = true, true, false) as user_has_completed_program
    from micromasters__program_enrollments
    left join mitx__programs
        on micromasters__program_enrollments.micromasters_program_id = mitx__programs.micromasters_program_id
    left join mitxonline__programs
        on mitx__programs.mitxonline_program_id = mitxonline__programs.program_id
    left join edx_program_certificates
        on
            micromasters__program_enrollments.micromasters_program_id = edx_program_certificates.micromasters_program_id
            and micromasters__program_enrollments.user_edxorg_id = edx_program_certificates.user_id
    left join edx_program_enrollments
        on
            micromasters__program_enrollments.micromasters_program_id = edx_program_enrollments.micromasters_program_id
            and micromasters__program_enrollments.user_edxorg_id = edx_program_enrollments.user_id
    where
        micromasters__program_enrollments.platform_name = '{{ var("edxorg") }}'
        and edx_program_enrollments.user_id is null
        and edx_program_enrollments.micromasters_program_id is null
)

select
    combined_programs.platform_name
    , combined_programs.program_id
    , combined_programs.program_title
    , combined_programs.program_name
    , combined_programs.program_track
    , combined_programs.program_type
    , combined_programs.program_is_live
    , combined_programs.program_readable_id
    , {{ generate_hash_id('cast(combined_programs.user_id as varchar) || combined_programs.platform_name') }}
    as user_hashed_id
    , combined_programs.user_id
    , combined_programs.user_email
    , combined_programs.user_username
    , combined_programs.user_full_name
    , combined_programs.user_has_completed_program
    , combined_programs.programenrollment_is_active
    , combined_programs.programenrollment_created_on
    , combined_programs.programenrollment_enrollment_status
    , combined_programs.programcertificate_created_on
    , combined_programs.programcertificate_is_revoked
    , combined_programs.programcertificate_uuid
    , combined_programs.programcertificate_url
from combined_programs

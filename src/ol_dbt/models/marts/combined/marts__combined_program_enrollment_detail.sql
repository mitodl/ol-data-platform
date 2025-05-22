with mitxpro__programenrollments as (
    select * from {{ ref('int__mitxpro__programenrollments') }}
)

, mitx_programs as (
    select * from {{ ref('int__mitx__program_requirements') }}
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

, edxorg_mitx_program_courses as (
    select * from {{ ref('int__edxorg__mitx_program_courses') }}
)

, mitx__courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, mitxpro__courses as (
    select * from {{ ref('int__mitxpro__courses') }}
)

, combined_enrollments as (
    select * from {{ ref('int__combined__courserun_enrollments') }}
)

, combined_courseruns as (
    select * from {{ ref('int__combined__course_runs') }}
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
        , {{ generate_micromasters_program_readable_id(
              'edx_program_enrollments.micromasters_program_id'
              ,'edx_program_enrollments.program_title')
         }} as program_readable_id
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

, courses_in_programs as (
    select
        mitx__courses.course_readable_id
        , mitxonline__programs.program_readable_id
    from mitx_programs
    inner join mitx__courses
        on mitx_programs.course_number = mitx__courses.course_number
    left join mitxonline__programs
        on mitx_programs.mitxonline_program_id = mitxonline__programs.program_id
    where mitx__courses.is_on_mitxonline = true

    union all

    select
        mitx__courses.course_readable_id
        , null as program_readable_id
    from mitx_programs
    inner join mitx__courses
        on mitx_programs.course_number = mitx__courses.course_number
    where mitx__courses.is_on_mitxonline = false

    union all

    select
        coalesce(mitx__courses.course_readable_id, edxorg_mitx_program_courses.course_readable_id) as course_readable_id
        , null as program_readable_id
    from edxorg_mitx_program_courses
    left join mitx_programs
        on edxorg_mitx_program_courses.program_name = mitx_programs.program_title
    left join mitx__courses
        on
            replace(edxorg_mitx_program_courses.course_readable_id, 'MITx/', '')
            = replace(mitx__courses.course_readable_id, 'course-v1:MITxT+', '')
    where mitx_programs.program_title is null

    union all

    select
        mitxpro__courses.course_readable_id
        , mitxpro__programs.program_readable_id
    from mitxpro__courses
    inner join mitxpro__programs
        on mitxpro__courses.program_id = mitxpro__programs.program_id
)

, final_combined_programs as (
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
        , cast(substring(combined_programs.programcertificate_created_on, 1, 10) as date)
        as programcertificate_created_on_date
        , sum(case when upper(combined_enrollments.course_title) like '%CAPSTONE%' then 1 else 0 end)
        as capstone_sum
        , min(cast(substring(combined_courseruns.courserun_start_on, 1, 10) as date))
        as first_courserun_start_on_date
        , count(distinct combined_enrollments.course_readable_id) as unique_courses_taken_in_program
    from combined_programs
    left join courses_in_programs
        on combined_programs.program_readable_id = courses_in_programs.program_readable_id
    left join combined_enrollments
        on
            cast(combined_programs.user_id as varchar) = combined_enrollments.user_id
            and courses_in_programs.course_readable_id = combined_enrollments.course_readable_id
    left join combined_courseruns
        on combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
    group by
        combined_programs.platform_name
        , combined_programs.program_id
        , combined_programs.program_title
        , combined_programs.program_name
        , combined_programs.program_track
        , combined_programs.program_type
        , combined_programs.program_is_live
        , combined_programs.program_readable_id
        , {{ generate_hash_id('cast(combined_programs.user_id as varchar) || combined_programs.platform_name') }}
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
        , cast(substring(combined_programs.programcertificate_created_on, 1, 10) as date)
)

select
    final_combined_programs.platform_name
    , final_combined_programs.program_id
    , final_combined_programs.program_title
    , final_combined_programs.program_name
    , final_combined_programs.program_track
    , final_combined_programs.program_type
    , final_combined_programs.program_is_live
    , final_combined_programs.program_readable_id
    , final_combined_programs.user_hashed_id
    , final_combined_programs.user_id
    , final_combined_programs.user_email
    , final_combined_programs.user_username
    , final_combined_programs.user_full_name
    , final_combined_programs.user_has_completed_program
    , final_combined_programs.programenrollment_is_active
    , final_combined_programs.programenrollment_created_on
    , final_combined_programs.programenrollment_enrollment_status
    , final_combined_programs.programcertificate_created_on
    , final_combined_programs.programcertificate_is_revoked
    , final_combined_programs.programcertificate_uuid
    , final_combined_programs.programcertificate_url
    , final_combined_programs.unique_courses_taken_in_program
    , case when final_combined_programs.capstone_sum > 0 then 'Y' else 'N' end
    as capstone_indicator
    , date_diff(
        'day'
        , final_combined_programs.programcertificate_created_on_date
        , final_combined_programs.first_courserun_start_on_date
    ) as program_completion_days
from final_combined_programs

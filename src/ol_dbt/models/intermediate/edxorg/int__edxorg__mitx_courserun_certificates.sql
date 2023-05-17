-- Course Certificate information for edx.org
-- for DEDP courses that were ran on edX.org, certificates are pull from MicroMasters.

with person_courses as (
    select *
    from {{ ref('stg__edxorg__bigquery__mitx_person_course') }}
    where
        courserun_platform = '{{ var("edxorg") }}'
        and courseruncertificate_is_earned = true
)

, user_info_combo as (
    select *
    from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
    where
        courserun_platform = '{{ var("edxorg") }}'
        and courseruncertificate_id is not null
)

, certificates as (
    select
        user_info_combo.courseruncertificate_id
        , user_info_combo.courseruncertificate_courserun_readable_id as courserun_readable_id
        , user_info_combo.courseruncertificate_mode
        , user_info_combo.courseruncertificate_grade
        , user_info_combo.courseruncertificate_download_url
        , user_info_combo.courseruncertificate_download_uuid
        , user_info_combo.courseruncertificate_verify_uuid
        , user_info_combo.courseruncertificate_name
        , user_info_combo.courseruncertificate_status
        , user_info_combo.courseruncertificate_created_on
        , user_info_combo.courseruncertificate_updated_on
        , user_info_combo.user_id
        , user_info_combo.user_email
        , user_info_combo.user_username
    from user_info_combo
    inner join person_courses -- this join is needed to filter out courseruncertificate that are not earned
        on
            user_info_combo.user_id = person_courses.user_id
            and user_info_combo.courseruncertificate_courserun_readable_id = person_courses.courserun_readable_id
)

, users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, runs as (
    select * from {{ ref('int__edxorg__mitx_courseruns') }}
)

, micromasters_users as (
    select * from {{ ref('__micromasters__users') }}
)

, edxorg_enrollments as (
    select * from {{ ref('int__edxorg__mitx_courserun_enrollments') }}
)

, dedp_edxorg_certificates_from_micromasters as (
    select * from {{ ref('__micromasters_course_certificates_dedp_from_micromasters') }}
    where courserun_platform = '{{ var("edxorg") }}'
)

, dedp_edxorg_grades_from_micromasters as (
    select * from {{ ref('__micromasters_course_grades_dedp_from_micromasters') }}
    where courserun_platform = '{{ var("edxorg") }}'
)

, edxorg_dedp_certificates_from_micromasters as (
    select
        dedp_edxorg_certificates_from_micromasters.courserun_edxorg_readable_id as courserun_readable_id
        , edxorg_enrollments.courserunenrollment_enrollment_mode as courseruncertificate_mode
        , dedp_edxorg_grades_from_micromasters.coursegrade_grade as courseruncertificate_grade
        , dedp_edxorg_certificates_from_micromasters.coursecertificate_url as courseruncertificate_download_url
        , dedp_edxorg_certificates_from_micromasters.coursecertificate_hash as courseruncertificate_download_uuid
        , dedp_edxorg_certificates_from_micromasters.coursecertificate_hash as courseruncertificate_verify_uuid
        , dedp_edxorg_certificates_from_micromasters.user_full_name as courseruncertificate_name
        , 'downloadable' as courseruncertificate_status
        , dedp_edxorg_certificates_from_micromasters.coursecertificate_created_on as courseruncertificate_created_on
        , dedp_edxorg_certificates_from_micromasters.coursecertificate_updated_on as courseruncertificate_updated_on
        , users.user_id
        , users.user_email
        , users.user_username
        , edxorg_enrollments.user_mitxonline_username
        , runs.courserun_title
        , coalesce(users.user_full_name, dedp_edxorg_certificates_from_micromasters.user_full_name) as user_full_name
    from dedp_edxorg_certificates_from_micromasters
    inner join users on dedp_edxorg_certificates_from_micromasters.user_edxorg_username = users.user_username
    left join
        runs
        on dedp_edxorg_certificates_from_micromasters.courserun_edxorg_readable_id = runs.courserun_readable_id
    left join dedp_edxorg_grades_from_micromasters
        on
            dedp_edxorg_certificates_from_micromasters.courserun_readable_id
            = dedp_edxorg_grades_from_micromasters.courserun_readable_id
            and dedp_edxorg_certificates_from_micromasters.user_email = dedp_edxorg_grades_from_micromasters.user_email
    left join edxorg_enrollments
        on
            dedp_edxorg_certificates_from_micromasters.courserun_edxorg_readable_id
            = edxorg_enrollments.courserun_readable_id
            and dedp_edxorg_certificates_from_micromasters.user_edxorg_username = edxorg_enrollments.user_username
)

, edxorg_non_dedp_certificates as (
    select
        certificates.courserun_readable_id
        , certificates.courseruncertificate_mode
        , certificates.courseruncertificate_grade
        , certificates.courseruncertificate_download_url
        , certificates.courseruncertificate_download_uuid
        , certificates.courseruncertificate_verify_uuid
        , certificates.courseruncertificate_name
        , certificates.courseruncertificate_status
        , certificates.courseruncertificate_created_on
        , certificates.courseruncertificate_updated_on
        , users.user_id
        , users.user_email
        , users.user_username
        , micromasters_users.user_mitxonline_username
        , runs.courserun_title
        , runs.micromasters_program_id
        , coalesce(users.user_full_name, micromasters_users.user_full_name) as user_full_name
    from certificates
    inner join users
        on certificates.user_id = users.user_id
    left join runs
        on certificates.courserun_readable_id = runs.courserun_readable_id
    left join micromasters_users
        on certificates.user_username = micromasters_users.user_edxorg_username
    where
        runs.micromasters_program_id != {{ var("dedp_micromasters_program_id") }}
        or runs.micromasters_program_id is null
)


select
    courserun_readable_id
    , courseruncertificate_mode
    , courseruncertificate_grade
    , courseruncertificate_download_url
    , courseruncertificate_download_uuid
    , courseruncertificate_verify_uuid
    , courseruncertificate_name
    , courseruncertificate_status
    , courseruncertificate_created_on
    , courseruncertificate_updated_on
    , user_id
    , user_email
    , user_username
    , user_full_name
    , user_mitxonline_username
    , courserun_title
    , micromasters_program_id
from edxorg_non_dedp_certificates

union all

select
    courserun_readable_id
    , courseruncertificate_mode
    , courseruncertificate_grade
    , courseruncertificate_download_url
    , courseruncertificate_download_uuid
    , courseruncertificate_verify_uuid
    , courseruncertificate_name
    , courseruncertificate_status
    , courseruncertificate_created_on
    , courseruncertificate_updated_on
    , user_id
    , user_email
    , user_username
    , user_full_name
    , user_mitxonline_username
    , courserun_title
    , {{ var("dedp_micromasters_program_id") }} as micromasters_program_id
from edxorg_dedp_certificates_from_micromasters

-- Course Certificate information for edx.org

with person_courses as (
    select *
    from dev.main_staging.stg__edxorg__bigquery__mitx_person_course
    where
        courserun_platform = 'edX.org'
        and courseruncertificate_is_earned = true
)

, user_info_combo as (
    select *
    from dev.main_staging.stg__edxorg__bigquery__mitx_user_info_combo
    where
        courserun_platform = 'edX.org'
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
    select * from dev.main_intermediate.int__edxorg__mitx_users
)

, runs as (
    select
        courserun_readable_id
        , courserun_title
    from dev.main_staging.stg__edxorg__bigquery__mitx_courserun
)

select distinct
    certificates.courseruncertificate_id
    , certificates.courserun_readable_id
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
    , runs.courserun_title
from certificates
inner join users
    on certificates.user_id = users.user_id
left join runs
    on certificates.courserun_readable_id = runs.courserun_readable_id

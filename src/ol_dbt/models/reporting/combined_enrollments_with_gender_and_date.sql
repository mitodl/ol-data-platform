with combined_users as (
    select * from {{ ref('marts__combined__users') }}
)

, enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

-- Fallback for edX.org enrollments. Once int__mitx__users links an edX.org account to a
-- MITx Online one, combined_users carries only the MITx Online user_hashed_id, so
-- enrollment rows keeping the edX.org hash no longer match. Resolve those by edX.org id.
-- Aggregated to one row per id: combined_users has a duplicate user_edxorg_id that would
-- otherwise fan out and inflate the row count.
, edxorg_user_profile as (
    select
        user_edxorg_id
        , max(user_job_title) as user_job_title
        , max(user_industry) as user_industry
    from combined_users
    where user_edxorg_id is not null
    group by user_edxorg_id
)

select
    enrollment_detail.platform
    , enrollment_detail.courserunenrollment_id
    , enrollment_detail.combined_orders_hash_id
    , enrollment_detail.course_readable_id
    , enrollment_detail.course_title
    , enrollment_detail.courserun_id
    , enrollment_detail.courserun_is_current
    , enrollment_detail.courserun_readable_id
    , enrollment_detail.courserun_start_on
    , enrollment_detail.courserun_end_on
    , enrollment_detail.courserun_title
    , enrollment_detail.courserun_upgrade_deadline
    , enrollment_detail.courseruncertificate_created_on
    , enrollment_detail.courseruncertificate_is_earned
    , enrollment_detail.courseruncertificate_url
    , enrollment_detail.courseruncertificate_uuid
    , enrollment_detail.courserunenrollment_created_on
    , enrollment_detail.courserunenrollment_enrollment_mode
    , enrollment_detail.courserunenrollment_enrollment_status
    , enrollment_detail.courserunenrollment_is_active
    , enrollment_detail.courserunenrollment_is_edx_enrolled
    , enrollment_detail.courserunenrollment_upgraded_on
    , enrollment_detail.courserungrade_grade
    , enrollment_detail.courserungrade_is_passing
    , enrollment_detail.line_id
    , enrollment_detail.order_id
    , enrollment_detail.order_reference_number
    , enrollment_detail.user_company
    , enrollment_detail.user_country_code
    , enrollment_detail.user_email
    , enrollment_detail.user_full_name
    , enrollment_detail.user_highest_education
    , enrollment_detail.user_hashed_id
    , enrollment_detail.user_id
    , enrollment_detail.user_username
    , nullif(enrollment_detail.user_gender, '') as user_gender
    , substring(courserunenrollment_created_on, 1, 10) as courserunenrollment_created_on_date
    , coalesce(combined_users.user_job_title, edxorg_user_profile.user_job_title) as user_job_title
    , coalesce(combined_users.user_industry, edxorg_user_profile.user_industry) as user_industry
from enrollment_detail
left join combined_users
    on enrollment_detail.user_hashed_id = combined_users.user_hashed_id
-- cast the bigint to varchar, not the reverse: user_id is shared by every platform and is
-- not guaranteed numeric on all of them.
left join edxorg_user_profile
    on enrollment_detail.platform = '{{ var("edxorg") }}'
    and enrollment_detail.user_id = cast(edxorg_user_profile.user_edxorg_id as varchar)

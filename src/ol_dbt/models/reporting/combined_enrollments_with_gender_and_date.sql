with combined_users as (
    select * from {{ ref('marts__combined__users') }}
)

, enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
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
    , combined_users.user_job_title
    , combined_users.user_industry
from enrollment_detail
left join combined_users
    on enrollment_detail.user_hashed_id = combined_users.user_hashed_id

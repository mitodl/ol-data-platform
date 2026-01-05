{{ config(
    materialized='table'
) }}

with enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, combined_users as (
    select * from {{ ref('marts__combined__users') }}
)

, mitxonline_video_engagements as (
    select * from {{ ref('marts__mitxonline_video_engagements') }}
)

, mitxonline_course_engagements_daily as (
    select * from {{ ref('marts__mitxonline_course_engagements_daily') }}
)

, mitxonline_problem_submissions as (
    select * from {{ ref('marts__mitxonline_problem_submissions') }}
)

, mitx_courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, base_enrollment_with_users as (
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
        , substring(enrollment_detail.courserunenrollment_created_on, 1, 10) as courserunenrollment_created_on_date
        , combined_users.user_job_title
        , combined_users.user_industry
        , combined_users.user_birth_year
        , combined_users.user_address_city
        , combined_users.user_address_state_or_territory
        , combined_users.user_address_postal_code
        , combined_users.user_street_address
        , combined_users.user_joined_on
        , combined_users.user_last_login
        , combined_users.user_is_active as user_is_active_on_platform
        , combined_users.num_of_course_enrolled
        , combined_users.num_of_course_passed
        , combined_users.first_course_start_datetime
        , combined_users.last_course_start_datetime
        , combined_users.number_of_courserun_certificates
        , combined_users.total_amount_paid_orders
        , combined_users.has_program_certificate
        , combined_users.has_dedp_program_certificate
        , mitx_courses.course_number
    from enrollment_detail
    left join combined_users
        on enrollment_detail.user_hashed_id = combined_users.user_hashed_id
    left join mitx_courses
        on enrollment_detail.course_readable_id = mitx_courses.course_readable_id
)

, video_engagement_stats as (
    select
        user_email
        , courserun_readable_id
        , count(distinct video_id) as total_videos_watched
        , count(distinct case when video_event_type = 'play_video' then video_id end) as videos_played_count
        , count(distinct case when video_event_type = 'complete_video' then video_id end) as videos_completed_count
        , count(distinct section_title) as sections_with_video_activity
        , max(coursestructure_block_index) as max_video_block_index_reached
    from mitxonline_video_engagements
    group by user_email, courserun_readable_id
)

, daily_engagement_stats as (
    select
        user_email
        , courserun_readable_id
        , count(distinct courseactivity_date) as total_days_active
        , sum(cast(num_events as bigint)) as total_activity_events
        , sum(cast(num_problem_submitted as bigint)) as total_problems_submitted
        , sum(cast(num_video_played as bigint)) as total_video_plays
        , sum(cast(num_discussion_participated as bigint)) as total_discussion_participations
        , max(courseactivity_date) as last_activity_date
        , min(courseactivity_date) as first_activity_date
    from mitxonline_course_engagements_daily
    group by user_email, courserun_readable_id
)

, problem_stats as (
    select
        user_email
        , courserun_readable_id
        , count(distinct problem_id) as total_problems_attempted
        , count(distinct case when problem_success = 'correct' then problem_id end) as problems_correct_count
        , count(distinct case when problem_success = 'incorrect' then problem_id end) as problems_incorrect_count
        , sum(cast(num_attempts as bigint)) as total_problem_attempts
        , avg(case when cast(problem_max_grade as double) > 0 then cast(problem_grade as double) / cast(problem_max_grade as double) end) as avg_problem_score_pct
    from mitxonline_problem_submissions
    where is_most_recent_attempt = true
    group by user_email, courserun_readable_id
)

select
    base.platform
    , base.courserunenrollment_id
    , base.combined_orders_hash_id
    , base.course_readable_id
    , base.course_title
    , base.course_number
    , base.courserun_id
    , base.courserun_is_current
    , base.courserun_readable_id
    , base.courserun_start_on
    , base.courserun_end_on
    , base.courserun_title
    , base.courserun_upgrade_deadline
    , base.courseruncertificate_created_on
    , base.courseruncertificate_is_earned
    , base.courseruncertificate_url
    , base.courseruncertificate_uuid
    , base.courserunenrollment_created_on
    , base.courserunenrollment_created_on_date
    , base.courserunenrollment_enrollment_mode
    , base.courserunenrollment_enrollment_status
    , base.courserunenrollment_is_active
    , base.courserunenrollment_is_edx_enrolled
    , base.courserunenrollment_upgraded_on
    , base.courserungrade_grade
    , base.courserungrade_is_passing
    , base.line_id
    , base.order_id
    , base.order_reference_number
    , base.user_hashed_id
    , base.user_id
    , base.user_username
    , base.user_email
    , base.user_full_name
    , base.user_gender
    , base.user_birth_year
    , base.user_company
    , base.user_job_title
    , base.user_industry
    , base.user_country_code
    , base.user_address_city
    , base.user_address_state_or_territory
    , base.user_address_postal_code
    , base.user_street_address
    , base.user_highest_education
    , base.user_joined_on
    , base.user_last_login
    , base.user_is_active_on_platform
    , base.num_of_course_enrolled
    , base.num_of_course_passed
    , base.first_course_start_datetime
    , base.last_course_start_datetime
    , base.number_of_courserun_certificates
    , base.total_amount_paid_orders
    , base.has_program_certificate
    , base.has_dedp_program_certificate
    , video_stats.total_videos_watched
    , video_stats.videos_played_count
    , video_stats.videos_completed_count
    , video_stats.sections_with_video_activity
    , video_stats.max_video_block_index_reached
    , daily_stats.total_days_active
    , daily_stats.total_activity_events
    , daily_stats.total_problems_submitted
    , daily_stats.total_video_plays
    , daily_stats.total_discussion_participations
    , daily_stats.first_activity_date
    , daily_stats.last_activity_date
    , problem_stats.total_problems_attempted
    , problem_stats.problems_correct_count
    , problem_stats.problems_incorrect_count
    , problem_stats.total_problem_attempts
    , problem_stats.avg_problem_score_pct
from base_enrollment_with_users as base
left join video_engagement_stats as video_stats
    on
        base.user_email = video_stats.user_email
        and base.courserun_readable_id = video_stats.courserun_readable_id
left join daily_engagement_stats as daily_stats
    on
        base.user_email = daily_stats.user_email
        and base.courserun_readable_id = daily_stats.courserun_readable_id
left join problem_stats
    on
        base.user_email = problem_stats.user_email
        and base.courserun_readable_id = problem_stats.courserun_readable_id

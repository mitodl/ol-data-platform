-- Enrollment activity per course run and day, with no learner columns, for the
-- course-run enrollment trends dashboard. The per-course [MMFin] chart copies each
-- restated these counts inline against one of four datasets; this defines them once.
-- Each count is of distinct enrollments whose event falls on activity_date, so a sum
-- over dates gives a run's total and a running sum gives the cumulative view.
with enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
    -- A row with no run can't be filtered to on the dashboard.
    where courserun_readable_id is not null
)

-- Taken once per run, so every day of a run carries the same values and a native
-- filter on them keeps or drops the whole run.
, course_run as (
    select
        platform
        , courserun_readable_id
        , max(course_readable_id) as course_readable_id
        , max(course_title) as course_title
        , max(courserun_title) as courserun_title
        , max(courserun_start_on) as courserun_start_on
        , max(courserun_end_on) as courserun_end_on
        , max(case when courserun_is_current then 1 else 0 end) = 1 as courserun_is_current
    from enrollment_detail
    group by platform, courserun_readable_id
)

, enrollment as (
    select
        enrollment_detail.platform
        , enrollment_detail.courserun_readable_id
        -- edX.org and Emeritus enrollments have no id of their own, and an Emeritus row
        -- may have no user_id either; the learner's email keeps those apart.
        , {{ dbt_utils.generate_surrogate_key([
            'enrollment_detail.platform'
            , 'enrollment_detail.courserunenrollment_id'
            , 'enrollment_detail.user_id'
            , 'enrollment_detail.user_email'
            , 'enrollment_detail.courserun_readable_id'
        ]) }} as enrollment_key
        , enrollment_detail.courserunenrollment_enrollment_mode as enrollment_mode
        , enrollment_detail.courserunenrollment_is_active as is_active
        , coalesce(verified_course_enrollments.is_verified, false) as is_verified
        , enrollment_detail.courseruncertificate_is_earned as has_certificate
        , enrollment_detail.courserunenrollment_created_on as enrolled_on
        -- Falls back to the enrollment for a platform that records no upgrade time
        -- (edX.org, residential, Emeritus, Global Alumni) or a verified enrollment
        -- with no order.
        , coalesce(
            enrollment_detail.courserunenrollment_upgraded_on
            , enrollment_detail.courserunenrollment_created_on
        ) as upgraded_on
        , enrollment_detail.courseruncertificate_created_on as certificate_created_on
    from enrollment_detail
    {{ join_verified_course_enrollments('enrollment_detail') }}
)

, activity as (
    select
        *
        , 'enrollment' as activity_type
        , cast(substring(enrolled_on, 1, 10) as date) as activity_date
    from enrollment

    union all

    select
        *
        , 'upgrade' as activity_type
        , cast(substring(upgraded_on, 1, 10) as date) as activity_date
    from enrollment
    where is_verified

    union all

    select
        *
        , 'certificate' as activity_type
        , cast(substring(certificate_created_on, 1, 10) as date) as activity_date
    from enrollment
    where has_certificate
)

, daily_activity as (
    select
        platform
        , courserun_readable_id
        , activity_date
        , count(distinct case when activity_type = 'enrollment' then enrollment_key end)
            as enrollments
        , count(distinct case when activity_type = 'enrollment' and is_active then enrollment_key end)
            as active_enrollments
        , count(distinct case when activity_type = 'enrollment' and not is_active then enrollment_key end)
            as unenrolled_enrollments
        , count(distinct case
            when activity_type = 'enrollment' and is_active and is_verified then enrollment_key
        end) as verified_enrollments
        , count(distinct case
            when activity_type = 'enrollment' and is_active and enrollment_mode = 'audit'
                then enrollment_key
        end) as audit_enrollments
        , count(distinct case when activity_type = 'upgrade' and is_active then enrollment_key end)
            as upgrades
        , count(distinct case when activity_type = 'certificate' then enrollment_key end)
            as certificates
    from activity
    -- Global Alumni records no enrollment time, so its enrollments have no day.
    where activity_date is not null
    group by platform, courserun_readable_id, activity_date
)

select
    daily_activity.platform
    , daily_activity.courserun_readable_id
    , daily_activity.activity_date
    , course_run.course_readable_id
    , course_run.course_title
    , course_run.courserun_title
    , course_run.courserun_start_on
    , course_run.courserun_end_on
    , course_run.courserun_is_current
    , daily_activity.enrollments
    , daily_activity.active_enrollments
    , daily_activity.unenrolled_enrollments
    , daily_activity.verified_enrollments
    , daily_activity.audit_enrollments
    , daily_activity.upgrades
    , daily_activity.certificates
from daily_activity
inner join course_run
    on
        daily_activity.platform = course_run.platform
        and daily_activity.courserun_readable_id = course_run.courserun_readable_id

-- Grain: one row per (user, course run). The one place the course completion status and
-- the needs-attention rule are defined; consumers select the columns and restate neither.
-- Rebuilt in full (the dimensional default): every status depends on the latest grade,
-- certificate and activity, and user_fk can re-key.
-- Consent is not applied here. A consumer-facing model built on this one must carry or
-- apply consent itself, as the B2B views do with outcomes_shared.
with enrollments_ranked as (
    select
        user_fk
        , courserun_fk
        , platform
        , enrollment_date_key
        , enrollment_created_on
        , enrollment_updated_on
        , enrollment_is_active
        , enrollment_mode
        , enrollment_status
        -- tfact_enrollment keeps one row per enrollment, and a refund, deferral or transfer
        -- leaves several per (user, course run). The active one is reported; failing that,
        -- the newest.
        , row_number() over (
            partition by user_fk, courserun_fk
            order by
                case when enrollment_is_active then 0 else 1 end
                , enrollment_created_on desc nulls last
                -- enrollment_id is a string; compare the integer ids as numbers.
                , try_cast(enrollment_id as bigint) desc nulls last
                , enrollment_id desc
        ) as enrollment_rank
    from {{ ref('tfact_enrollment') }}
    where enrollment_type = 'course'
      and user_fk is not null
      and courserun_fk is not null
)

, enrollments as (
    select *
    from enrollments_ranked
    where enrollment_rank = 1
)

, certificates as (
    select
        user_fk
        , courserun_fk
        , certificate_is_revoked
        , certificate_issued_on
        , certificate_updated_on
    from {{ ref('tfact_certificate') }}
    -- is_current leaves one certificate per (user, course run): the unrevoked one, then
    -- the latest issued.
    where certificate_scope = 'course'
      and is_current
)

-- activity_date_key is YYYYMMDD, so its max is the latest day.
, activity as (
    select
        user_fk
        , courserun_fk
        , max(activity_date_key) as last_active_date_key
    from {{ ref('afact_learner_courserun_daily_activity') }}
    where courserun_fk is not null
    group by user_fk, courserun_fk
)

-- A unit is an Open edX vertical with at least one block that counts toward progress.
, content_totals as (
    select
        courserun_fk
        , count(distinct vertical_block_id) as content_units_total
    from {{ ref('dim_course_content_progress_block') }}
    where courserun_fk is not null
    group by courserun_fk
)

, joined as (
    select
        enrollments.user_fk
        , enrollments.courserun_fk
        , enrollments.platform
        , enrollments.enrollment_created_on
        , enrollments.enrollment_updated_on
        , enrollments.enrollment_is_active
        , enrollments.enrollment_mode
        , enrollments.enrollment_status
        , grades.is_passing
        , grades.grade_value
        , grades.letter_grade
        , grades.grade_updated_on
        , certificates.certificate_is_revoked
        , certificates.certificate_issued_on
        , certificates.certificate_updated_on
        , coalesce(certificates.certificate_is_revoked = false, false) as is_certified
        , cast(enrollment_dates.date as date) as enrolled_on
        , cast(activity_dates.date as date) as last_active_on
        , content_totals.content_units_total
        -- Null only when the run has no course structure to count against.
        , case
            when content_totals.content_units_total is not null
                then coalesce(content_progress.content_units_completed, 0)
        end as content_units_completed
    from enrollments
    left join {{ ref('tfact_grade') }} as grades
        on enrollments.user_fk = grades.user_fk
        and enrollments.courserun_fk = grades.courserun_fk
    left join certificates
        on enrollments.user_fk = certificates.user_fk
        and enrollments.courserun_fk = certificates.courserun_fk
    left join activity
        on enrollments.user_fk = activity.user_fk
        and enrollments.courserun_fk = activity.courserun_fk
    left join content_totals
        on enrollments.courserun_fk = content_totals.courserun_fk
    left join {{ ref('afact_learner_courserun_content_progress') }} as content_progress
        on enrollments.user_fk = content_progress.user_fk
        and enrollments.courserun_fk = content_progress.courserun_fk
    left join {{ ref('dim_date') }} as enrollment_dates
        on enrollments.enrollment_date_key = enrollment_dates.date_key
    left join {{ ref('dim_date') }} as activity_dates
        on activity.last_active_date_key = activity_dates.date_key
)

, statuses as (
    select
        *
        -- An unrevoked certificate is certified without requiring is_passing (#2669), and
        -- in_progress needs a nonzero grade or tracked activity (#2693).
        , case
            when is_certified then 'certified'
            when is_passing then 'passed'
            when grade_value > 0 or last_active_on is not null then 'in_progress'
            else 'not_started'
        end as completion_status
    from joined
)

select
    user_fk
    , courserun_fk
    , platform
    , enrollment_created_on
    , enrollment_updated_on
    , enrollment_is_active
    , enrollment_mode
    , enrollment_status
    , is_passing
    , grade_value
    , letter_grade
    , grade_updated_on
    , certificate_is_revoked
    , certificate_issued_on
    , certificate_updated_on
    , is_certified
    , last_active_on
    , completion_status
    , completion_status = 'in_progress' as is_in_progress
    , completion_status = 'not_started' as is_not_started
    , content_units_completed
    , content_units_total
    -- 0 to 1, like grade_value. Independent of completion_status: a learner can be
    -- certified without opening every block.
    , cast(content_units_completed as double) / nullif(content_units_total, 0) as content_progress
    -- A threshold date, not a flag: the consumer compares it against the current UTC date,
    -- so the answer does not freeze at this build. A learner who never started needs
    -- attention from the day they enrolled; the fallback keeps that true when the
    -- enrollment has no usable created date. A learner in progress needs attention once
    -- they have been quiet for needs_attention_quiet_days, which cannot be dated when the
    -- only evidence of progress is a grade. Going quiet after passing is expected.
    , case
        when completion_status = 'not_started'
            then coalesce(enrolled_on, cast('1970-01-01' as date))
        when completion_status = 'in_progress'
            then cast(
                {{ dbt.dateadd('day', var('needs_attention_quiet_days'), 'last_active_on') }}
                as date
            )
    end as needs_attention_since
from statuses

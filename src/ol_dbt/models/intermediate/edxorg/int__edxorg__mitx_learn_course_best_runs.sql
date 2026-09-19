{#
  One row per MITx edX.org catalog course, as MIT Learn's mit_edx course ETL loads it,
  with the course's best run. The best run is MIT Learn's LearningResource.best_run:
  of the published runs, the earliest-starting one open for enrollment now, else the
  next to start, else the latest to have started. MIT Learn copies a course's
  duration and weekly hours from its best run, and programs take their instructors
  from it. Evaluated when the model is built.
#}

with runs as (
    select
        course_readable_id
        , courserun_readable_id
        , courserun_title
        , courserun_is_published
        -- MIT Learn falls back to the enrollment start when a run has no start
        , {{ from_iso8601_timestamp('coalesce(courserun_start_on, courserun_enrollment_start_on)') }} as start_on
        , {{ from_iso8601_timestamp('courserun_end_on') }} as end_on
        , {{ from_iso8601_timestamp('courserun_enrollment_start_on') }} as enrollment_start_on
        , {{ from_iso8601_timestamp('courserun_enrollment_end_on') }} as enrollment_end_on
        , courserun_min_weekly_hours
        , courserun_max_weekly_hours
        , courserun_weeks_to_complete
        , courserun_instructors
    from {{ ref('stg__edxorg__api__courserun') }}
    -- runs MIT Learn skips as deleted
    where not (
        lower(trim(courserun_title)) like '%[delete]%'
        or lower(trim(courserun_title)) like '%(delete)%'
        or lower(trim(courserun_title)) like '%delete %'
        or lower(trim(courserun_title)) = 'delete'
    )
)

, ranked_runs as (
    select
        *
        , case
            when
                (
                    (enrollment_start_on is not null and enrollment_start_on <= current_timestamp)
                    or (enrollment_start_on is null and start_on is not null and start_on <= current_timestamp)
                )
                and (
                    (enrollment_end_on is not null and enrollment_end_on > current_timestamp)
                    or (enrollment_end_on is null and end_on is not null and end_on > current_timestamp)
                )
                then 1
            when start_on >= current_timestamp then 2
            when start_on is not null then 3
            else 4
        end as best_run_tier
    from runs
    where courserun_is_published
)

, best_runs as (
    select *
    from (
        select
            *
            , row_number() over (
                partition by course_readable_id
                order by
                    best_run_tier
                    -- enrollable: earliest start then end; upcoming: earliest start;
                    -- started: latest start
                    , case when best_run_tier in (1, 2) then coalesce(start_on, current_timestamp) end asc
                    , case when best_run_tier = 1 then coalesce(end_on, current_timestamp) end asc
                    , case when best_run_tier = 3 then start_on end desc
                    , courserun_readable_id
            ) as best_run_rank
        from ranked_runs
    )
    where best_run_rank = 1
)

select
    runs_by_course.course_readable_id
    -- MIT Learn publishes a course when any of its runs is published
    , runs_by_course.is_published
    , best_runs.courserun_readable_id as best_run_readable_id
    -- as MIT Learn's commitment parsing: a missing minimum counts as 0 and a missing
    -- maximum as the minimum
    , case
        when coalesce(best_runs.courserun_min_weekly_hours, 0) != 0
            or coalesce(best_runs.courserun_max_weekly_hours, best_runs.courserun_min_weekly_hours, 0) != 0
            then least(
                coalesce(best_runs.courserun_min_weekly_hours, 0)
                , coalesce(best_runs.courserun_max_weekly_hours, best_runs.courserun_min_weekly_hours, 0)
            )
    end as best_run_min_weekly_hours
    , case
        when coalesce(best_runs.courserun_min_weekly_hours, 0) != 0
            or coalesce(best_runs.courserun_max_weekly_hours, best_runs.courserun_min_weekly_hours, 0) != 0
            then greatest(
                coalesce(best_runs.courserun_min_weekly_hours, 0)
                , coalesce(best_runs.courserun_max_weekly_hours, best_runs.courserun_min_weekly_hours, 0)
            )
    end as best_run_max_weekly_hours
    , nullif(best_runs.courserun_weeks_to_complete, 0) as best_run_max_weeks
    , best_runs.courserun_instructors as best_run_instructors_json
from (
    select
        course_readable_id
        , bool_or(courserun_is_published) as is_published
    from runs
    group by course_readable_id
) as runs_by_course
left join best_runs on runs_by_course.course_readable_id = best_runs.course_readable_id

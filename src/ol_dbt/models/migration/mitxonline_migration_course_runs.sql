with xpro_migration_runs as (
    select distinct courserun_readable_id
    from {{ ref('mitxonline_migration_course_enrollments') }}
    where source = 'xpro'
)

, xpro_runs as (
    select
        runs.courserun_readable_id
        , runs.courserun_title
        , runs.courserun_is_live
        -- A few xPRO runs have no course in dim_course; their key is the run key minus the run tag
        , coalesce(
            courses.course_readable_id
            , {{ regexp_replace_all('runs.courserun_readable_id', "'[+][^+]+$'", "''") }}
        ) as course_readable_id
        , coalesce(courses.course_title, runs.courserun_title) as course_title
        , {{ from_iso8601_timestamp('runs.enrollment_start') }} as enrollment_start
        , {{ from_iso8601_timestamp('coalesce(runs.enrollment_end, runs.courserun_end_on)') }} as enrollment_end
        , {{ from_iso8601_timestamp('runs.courserun_start_on') }} as start_date
        , {{ from_iso8601_timestamp('runs.courserun_end_on') }} as end_date
        , {{ from_iso8601_timestamp('runs.courserun_created_on') }} as created_on
    from {{ ref('dim_course_run') }} as runs
    inner join xpro_migration_runs
        on runs.courserun_readable_id = xpro_migration_runs.courserun_readable_id
    left join {{ ref('dim_course') }} as courses
        on
            runs.course_fk = courses.course_pk
            and courses.is_current = true
    where runs.platform = 'mitxpro' and runs.is_current = true
)

, xpro_course_runs as (
    select
        'xpro' as source
        , mitxonline_courses.source_id as mitxonline_course_id
        , xpro_runs.course_readable_id
        , xpro_runs.course_title
        , xpro_runs.courserun_title
        , xpro_runs.courserun_readable_id as courseware_id
        , {{ regexp_extract_or_null('xpro_runs.courserun_readable_id', "'[^+]+$'") }} as run_tag
        , xpro_runs.enrollment_start
        -- Closed on MITx Online while xPRO still sells the run: an open run ends at a
        -- past date that stays the same from day to day
        , case
            when xpro_runs.enrollment_end < current_timestamp then xpro_runs.enrollment_end
            when xpro_runs.enrollment_start < current_timestamp then xpro_runs.enrollment_start
            else xpro_runs.created_on
        end as enrollment_end
        , xpro_runs.start_date
        , xpro_runs.end_date
        -- xPRO has no self-paced flag; its runs are instructor-paced
        , false as is_self_paced
        , xpro_runs.courserun_is_live as is_published
    from xpro_runs
    left join {{ ref('dim_course_run') }} as mitxonline_runs
        on
            xpro_runs.courserun_readable_id = mitxonline_runs.courserun_readable_id
            and mitxonline_runs.platform = 'mitxonline'
            and mitxonline_runs.is_current = true
    left join {{ ref('dim_course') }} as mitxonline_courses
        on
            xpro_runs.course_readable_id = mitxonline_courses.course_readable_id
            and mitxonline_courses.primary_platform = 'mitxonline'
            and mitxonline_courses.is_current = true
    -- Only runs not in MITx Online yet; the task does not update existing runs
    where mitxonline_runs.courserun_pk is null
)

-- Add each new source as a CTE and union all it here
select
    source
    , mitxonline_course_id
    , course_readable_id
    , course_title
    , courserun_title
    , courseware_id
    , run_tag
    , enrollment_start
    , enrollment_end
    , start_date
    , end_date
    , is_self_paced
    , is_published
from xpro_course_runs

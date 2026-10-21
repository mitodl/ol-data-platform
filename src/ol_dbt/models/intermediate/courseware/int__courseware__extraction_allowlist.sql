-- One row per course run that courseware_extraction_allowlist approves for claim
-- extraction. A run takes its own row, else its course's (through
-- dim_course_run.course_fk, so a run with no dim_course course matches only run and
-- platform rows), else its platform's. A run row approves its run even when
-- dim_course_run has no current row for it.
with allowlist as (
    select * from {{ ref('courseware_extraction_allowlist') }}
)

, courseruns as (
    select
        dim_course_run.platform
        , dim_course_run.courserun_readable_id
        , dim_course.course_readable_id
    from {{ ref('dim_course_run') }} as dim_course_run
    left join {{ ref('dim_course') }} as dim_course
        on dim_course_run.course_fk = dim_course.course_pk
    where
        dim_course_run.is_current
        and dim_course_run.platform in ('mitxonline', 'mitxpro', 'residential')
)

, candidate_runs as (
    select
        platform
        , courserun_readable_id
        , course_readable_id
    from courseruns

    union all

    select
        platform
        , courserun_readable_id
        , cast(null as varchar) as course_readable_id
    from allowlist
    where courserun_readable_id is not null
)

, matches as (
    select
        candidate_runs.platform
        , candidate_runs.courserun_readable_id
        , case
            when allowlist.courserun_readable_id is not null then 'courserun'
            when allowlist.course_readable_id is not null then 'course'
            else 'platform'
        end as allowlist_level
        , row_number() over (
            partition by candidate_runs.platform, candidate_runs.courserun_readable_id
            order by
                case
                    when allowlist.courserun_readable_id is not null then 1
                    when allowlist.course_readable_id is not null then 2
                    else 3
                end
        ) as match_rank
    from candidate_runs
    inner join allowlist
        on
            candidate_runs.platform = allowlist.platform
            and (
                allowlist.courserun_readable_id = candidate_runs.courserun_readable_id
                or (
                    allowlist.courserun_readable_id is null
                    and allowlist.course_readable_id = candidate_runs.course_readable_id
                )
                or (
                    allowlist.courserun_readable_id is null
                    and allowlist.course_readable_id is null
                )
            )
)

select
    platform
    , courserun_readable_id
    , allowlist_level
from matches
where match_rank = 1

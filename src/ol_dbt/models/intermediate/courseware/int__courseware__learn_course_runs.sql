{#
    For each course Learn lists, the run whose extraction a knowledge release
    publishes: the source-eligible run with the newest extraction, keyed as Learn
    keys the course (etl_source, readable_id). Shared by
    int__courseware__release_resource_knowledge and
    int__courseware__release_resource_concepts so the two describe the same run.
#}
with eligible_runs as (
    select * from {{ ref('int__courseware__source_eligibility') }}
    where is_source_eligible
)

, extractions as (
    select extractions.*
    from {{ ref('dim_courseware_extraction') }} as extractions
    inner join eligible_runs
        on
            extractions.platform = eligible_runs.platform
            and extractions.courserun_readable_id = eligible_runs.courserun_readable_id
    where extractions.is_current
)

, course_runs as (
    select * from {{ ref('dim_course_run') }}
    where is_current
)

, courses as (
    select * from {{ ref('dim_course') }}
    where is_current
)

, extracted_runs as (
    select
        platform
        , courserun_readable_id
        , courserun_fk
        , course_fk
        , row_number() over (
            partition by course_fk order by last_extracted_at desc
        ) as run_rank
    from (
        select
            extractions.platform
            , extractions.courserun_readable_id
            , extractions.courserun_fk
            , course_runs.course_fk
            , max(extractions.extracted_at) as last_extracted_at
        from extractions
        inner join course_runs
            on extractions.courserun_fk = course_runs.courserun_pk
        group by
            extractions.platform
            , extractions.courserun_readable_id
            , extractions.courserun_fk
            , course_runs.course_fk
    ) as runs
)

select
    case extracted_runs.platform
        when 'mitxonline' then 'mitxonline'
        when 'mitxpro' then 'xpro'
    end as etl_source
    , courses.course_readable_id as readable_id
    , courses.course_title
    , extracted_runs.courserun_fk
    , extracted_runs.courserun_readable_id
from extracted_runs
inner join courses
    on extracted_runs.course_fk = courses.course_pk
where
    extracted_runs.run_rank = 1
    and extracted_runs.platform in ('mitxonline', 'mitxpro')

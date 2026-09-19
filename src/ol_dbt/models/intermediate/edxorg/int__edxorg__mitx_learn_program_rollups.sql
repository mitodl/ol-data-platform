{#
  What an edX.org program takes from its courses as MIT Learn lists them, one row per
  program in int__edxorg__mitx_learn_programs. MIT Learn's legacy ETL read these back
  from its own course records; here they come from the same catalog data those records
  were loaded from (int__edxorg__mitx_learn_course_best_runs). Only courses MIT Learn
  publishes count.
  - topics: each course's edX subjects resolved through the MIT Learn topic crosswalk
    for offeror mitx;
  - duration: the courses' weeks to complete, summed;
  - weekly hours: the courses' minimum and maximum weekly hours, each averaged and
    rounded half to even as Python's round() does.
  Instructors are in int__edxorg__mitx_learn_program_instructors.
#}

with programs as (
    select * from {{ ref('int__edxorg__mitx_learn_programs') }}
)

, program_courses as (
    select *
    from {{ ref('stg__edxorg__s3__program_courses') }}
    where program_course_retrieved_at = (
        select max(program_course_retrieved_at) from {{ ref('stg__edxorg__s3__program_courses') }}
    )
)

, published_courses as (
    select
        program_courses.program_uuid
        , courses.course_readable_id
        , courses.best_run_min_weekly_hours
        , courses.best_run_max_weekly_hours
        , courses.best_run_max_weeks
    from program_courses
    inner join programs on program_courses.program_uuid = programs.readable_id
    inner join {{ ref('int__edxorg__mitx_learn_course_best_runs') }} as courses
        on program_courses.course_key = courses.course_readable_id
    where courses.is_published
)

, course_topics as (
    select
        published_courses.program_uuid
        , subject.topic_name as offeror_topic_name
    from published_courses
    inner join {{ ref('stg__edxorg__api__course') }} as catalog_courses
        on published_courses.course_readable_id = catalog_courses.course_readable_id
    cross join unnest(catalog_courses.course_topics) as subject (topic_name)
)

, program_topics as (
    select
        course_topics.program_uuid
        , array_agg(distinct lookup.topic_name order by lookup.topic_name) as topics
    from course_topics
    inner join {{ ref('int__learn__offeror_topic_lookup') }} as lookup
        on course_topics.offeror_topic_name = lookup.offeror_topic_name
        and lookup.offeror_code = 'mitx'
    group by course_topics.program_uuid
)

, program_effort as (
    select
        program_uuid
        , sum(nullif(best_run_max_weeks, 0)) as total_weeks
        , avg(cast(nullif(best_run_min_weekly_hours, 0) as double)) as avg_min_weekly_hours
        , avg(cast(nullif(best_run_max_weekly_hours, 0) as double)) as avg_max_weekly_hours
    from published_courses
    group by program_uuid
)

, rounded_effort as (
    select
        program_uuid
        , total_weeks
        {% for column in ['avg_min_weekly_hours', 'avg_max_weekly_hours'] %}
        , case
            when {{ column }} - floor({{ column }}) = 0.5
                then cast(floor({{ column }}) as integer) + mod(cast(floor({{ column }}) as integer), 2)
            else cast(round({{ column }}) as integer)
        end as {{ column | replace('avg_', '') }}
        {% endfor %}
    from program_effort
)

, commitment as (
    -- MIT Learn's commitment parsing: a missing minimum counts as 0, a missing maximum
    -- as the minimum
    select
        program_uuid
        , total_weeks
        , coalesce(min_weekly_hours, 0) as effort_min
        , coalesce(max_weekly_hours, min_weekly_hours, 0) as effort_max
    from rounded_effort
)

select
    programs.readable_id
    , coalesce(program_topics.topics, {{ null_varchar_array() }}) as topics
    , case
        when commitment.total_weeks > 0
            then cast(commitment.total_weeks as varchar)
            || case when commitment.total_weeks > 1 then ' weeks' else ' week' end
        else ''
    end as duration
    , case when commitment.total_weeks > 0 then commitment.total_weeks end as min_weeks
    , case when commitment.total_weeks > 0 then commitment.total_weeks end as max_weeks
    , case
        when commitment.effort_min != 0 or commitment.effort_max != 0
            then
                case
                    when commitment.effort_min != commitment.effort_max
                        then cast(commitment.effort_min as varchar) || '-'
                    else ''
                end
                || cast(commitment.effort_max as varchar)
                || case when commitment.effort_max > 1 then ' hours/week' else ' hour/week' end
        else ''
    end as time_commitment
    , case
        when commitment.effort_min != 0 or commitment.effort_max != 0
            then least(commitment.effort_min, commitment.effort_max)
    end as min_weekly_hours
    , case
        when commitment.effort_min != 0 or commitment.effort_max != 0
            then greatest(commitment.effort_min, commitment.effort_max)
    end as max_weekly_hours
from programs
left join program_topics on programs.readable_id = program_topics.program_uuid
left join commitment on programs.readable_id = commitment.program_uuid

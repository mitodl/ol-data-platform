{#
  One row per Sloan Executive Education course MIT Learn's legacy Sloan ETL would
  load (transform_courses in learning_resources/etl/sloan.py): a course with a URL and
  at least one offering. Courses it skipped are left out. Runs are in
  int__see__course_runs.
#}

with courses as (
    select * from {{ ref('stg__see__api__courses') }}
)

, runs as (
    select * from {{ ref('int__see__course_runs') }}
)

, topic_lookup as (
    select * from {{ ref('int__learn__offeror_topic_lookup') }}
    where offeror_code = 'see'
)

, run_rollups as (
    select
        readable_id
        , array_agg(distinct delivery order by delivery) as delivery
        , array_agg(distinct pace order by pace) as pace
        , bool_or(is_asynchronous) as has_asynchronous_run
        , bool_or(is_synchronous) as has_synchronous_run
    from runs
    group by readable_id
)

-- MIT Learn took the credits of the course's first offering in API order, null
-- included. min_by would skip a null on DuckDB.
, first_runs as (
    select
        readable_id
        , continuing_ed_credits
        , row_number() over (partition by readable_id order by run_api_position) as run_rank
    from runs
)

-- Sloan gives one "Category: Topic" per course. MIT Learn kept the part after the
-- last colon and resolved it through the offeror's topic mappings.
, topics as (
    select
        courses.course_id as readable_id
        , array_agg(distinct topic_lookup.topic_name order by topic_lookup.topic_name) as topics
    from courses
    inner join topic_lookup
        on trim({{ regexp_extract_or_null('courses.course_topic_raw', "'([^:]*)$'", 1) }})
        = topic_lookup.offeror_topic_name
    group by courses.course_id
)

select
    courses.course_id as readable_id
    , courses.course_title as title
    , courses.course_description as description
    , courses.course_url as url
    , courses.course_image_url as image_url
    , courses.course_title as image_alt
    , courses.course_certification_type as sloan_certification_type
    , courses.course_updated_on as updated_on
    , topics.topics
    , run_rollups.delivery
    , run_rollups.pace
    , split(concat_ws(
        ','
        , case when run_rollups.has_asynchronous_run then 'asynchronous' end
        , case when run_rollups.has_synchronous_run then 'synchronous' end
    ), ',') as format
    , first_runs.continuing_ed_credits
from courses
inner join run_rollups on courses.course_id = run_rollups.readable_id
inner join first_runs on courses.course_id = first_runs.readable_id and first_runs.run_rank = 1
left join topics on courses.course_id = topics.readable_id
where courses.course_url is not null and courses.course_url != ''

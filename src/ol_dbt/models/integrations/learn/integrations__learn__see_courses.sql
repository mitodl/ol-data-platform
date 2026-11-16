{#
  integrations__learn__see_courses
  Sloan Executive Education courses for MIT Learn: every course MIT Learn's legacy
  Sloan ETL would load (transform_courses in learning_resources/etl/sloan.py), a course
  with a URL and at least one offering, with the course-level fields transform_course
  set. A course missing here is one MIT Learn treats as unpublished. Runs are in
  integrations__learn__see_runs.
  Contract: docs/learn_marts_contract.md
#}

with courses as (
    select * from {{ ref('int__see__courses') }}
    where has_url and has_offerings
)

, runs as (
    select
        readable_id
        , run_api_position
        , continuing_ed_credits
        , is_synchronous
        , is_asynchronous
        , {{ learn_delivery('delivery_label') }} as delivery
        , case when is_on_demand then 'self_paced' else 'instructor_paced' end as pace
    from {{ ref('int__see__course_runs') }}
)

, topic_lookup as (
    select * from {{ ref('int__learn__offeror_topic_lookup') }}
    where offeror_code = 'see'
)

, run_rollups as (
    select
        readable_id
        , array_agg(distinct delivery order by delivery nulls last) as delivery
        , array_agg(distinct pace order by pace nulls last) as pace
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
        , row_number() over (partition by readable_id order by run_api_position nulls last) as run_rank
    from runs
)

-- MIT Learn resolved the Sloan topic through the offeror's topic mappings and dropped
-- one with no mapping.
, topics as (
    select
        courses.readable_id
        , array_agg(distinct topic_lookup.topic_name order by topic_lookup.topic_name nulls last) as topics
    from courses
    inner join topic_lookup on courses.topic = topic_lookup.offeror_topic_name
    group by courses.readable_id
)

select
    courses.readable_id
    , courses.title
    , {{ cast_timestamp_to_iso8601('courses.updated_on') }} as last_modified
    , courses.description
    , courses.url
    , courses.image_url
    , courses.title as image_alt
    , topics.topics
    , run_rollups.delivery
    , run_rollups.pace
    , split(concat_ws(
        ','
        , case when run_rollups.has_asynchronous_run then 'asynchronous' end
        , case when run_rollups.has_synchronous_run then 'synchronous' end
    ), ',') as format
    , first_runs.continuing_ed_credits
    , true as certification
    , 'professional' as certification_type
    , true as professional
    , true as published
    , 'see' as etl_source
    , 'see' as platform
    , 'see' as offered_by
    , 'course' as resource_type
from courses
inner join run_rollups on courses.readable_id = run_rollups.readable_id
inner join first_runs on courses.readable_id = first_runs.readable_id and first_runs.run_rank = 1
left join topics on courses.readable_id = topics.readable_id

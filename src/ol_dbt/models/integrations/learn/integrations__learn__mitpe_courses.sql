{#
  integrations__learn__mitpe_courses
  MIT Professional Education courses for MIT Learn webhook delivery: every course
  with at least one published run (integrations__learn__mitpe_runs). A course with
  none is left out, which MIT Learn treats as unpublished.
  Contract: docs/learn_marts_contract.md
#}

with resources as (
    select * from {{ ref('int__mitpe__learning_resources') }}
)

, published_resources as (
    select distinct readable_id
    from {{ ref('int__mitpe__learning_resource_runs') }}
    where is_published
)

select
    resources.readable_id
    , resources.title
    , resources.url
    , resources.image_url
    , resources.image_alt
    , resources.description
    , resources.topics
    , resources.delivery
    , resources.location
    , resources.duration
    , resources.min_weeks
    , resources.max_weeks
    , resources.price
    , resources.instructors
    , 'mitpe' as etl_source
    , 'mitpe' as platform
    , 'course' as resource_type
from resources
inner join published_resources on resources.readable_id = published_resources.readable_id
where resources.resource_type = 'course'

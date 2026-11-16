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
    from {{ ref('integrations__learn__mitpe_runs') }}
)

select
    resources.readable_id
    , resources.title
    , resources.url
    , resources.image_url
    , resources.image_alt
    , resources.description
    , resources.topics
    -- MIT Learn's shaping: delivery codes (unknown formats are online), location only
    -- for on-site formats, empty strings for missing text, Learn's duration weeks
    , {{ learn_delivery('resources.learning_format') }} as delivery
    , {{ learn_mitpe_location('resources.learning_format', 'resources.location') }} as location
    , coalesce(resources.duration, '') as duration
    , {{ learn_duration_weeks('resources.duration', 'min') }} as min_weeks
    , {{ learn_duration_weeks('resources.duration', 'max') }} as max_weeks
    , {{ learn_mitpe_price('resources.price_text') }} as price
    , resources.instructors
    , 'mitpe' as etl_source
    , 'mitpe' as platform
    , 'course' as resource_type
from resources
inner join published_resources on resources.readable_id = published_resources.readable_id
where resources.resource_type = 'course'

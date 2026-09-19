{#
  integrations__learn__mitpe_programs
  MIT Professional Education programs for MIT Learn webhook delivery: every program
  with at least one published run, with the delivered courses it contains in the
  order the feed lists them.
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

, program_courses as (
    select
        program_courses.program_readable_id
        , array_agg(program_courses.course_readable_id order by program_courses.course_position)
            as course_readable_ids
    from {{ ref('int__mitpe__program_courses') }} as program_courses
    inner join {{ ref('integrations__learn__mitpe_courses') }} as courses
        on program_courses.course_readable_id = courses.readable_id
    group by program_courses.program_readable_id
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
    , program_courses.course_readable_ids
    , 'mitpe' as etl_source
    , 'mitpe' as platform
    , 'program' as resource_type
from resources
inner join published_resources on resources.readable_id = published_resources.readable_id
left join program_courses on resources.readable_id = program_courses.program_readable_id
where resources.resource_type = 'program'

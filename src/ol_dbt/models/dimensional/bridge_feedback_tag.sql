{{ config(
    materialized='table'
) }}

-- One row per turn and tag, so you can filter turns by tag without joining the fact.
with unioned as (
    select
        source_slug
        , source_tags
        , {{ dbt_utils.generate_surrogate_key(['source_slug', 'source_record_ref']) }}
            as feedback_pk
    from {{ ref('int__feedback__unioned') }}
    where source_tags is not null
)

, feedback_tag as (
    select * from {{ ref('dim_feedback_tag') }}
)

, feedback as (
    select feedback_pk from {{ ref('tfact_feedback') }}
)

-- Joins to tfact_feedback so a turn only appears once its fact row exists.
, exploded as (
    select
        unioned.feedback_pk
        , unioned.source_slug
        , {{ slugify('tag.tag_label') }}
            as tag_slug
    from unioned
    inner join feedback
        on feedback.feedback_pk = unioned.feedback_pk
    cross join unnest(unioned.source_tags) as tag (tag_label)
    where tag.tag_label is not null
        and tag.tag_label != ''
)

select distinct
    exploded.feedback_pk
    , feedback_tag.feedback_tag_pk
from exploded
inner join feedback_tag
    on exploded.tag_slug = feedback_tag.tag_slug
    and exploded.source_slug = feedback_tag.source_slug

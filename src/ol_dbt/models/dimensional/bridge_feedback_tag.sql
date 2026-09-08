{{ config(
    materialized='table'
) }}

-- One row per turn and tag, so you can filter turns by tag without joining the fact.
with unioned as (
    select
        source_slug
        , source_record_ref
        , source_tags
        , {{ dbt_utils.generate_surrogate_key(['source_slug']) }} as feedback_source_fk
    from {{ ref('int__feedback__unioned') }}
    where source_tags is not null
)

, feedback_tag as (
    select * from {{ ref('dim_feedback_tag') }}
)

, feedback as (
    select
        feedback_pk
        , source_record_id
        , feedback_source_fk
    from {{ ref('tfact_feedback') }}
)

-- Joins to tfact_feedback so a turn only appears once its fact row exists, and takes
-- feedback_pk from there rather than rederiving it, so a change to the fact's key
-- formula can't silently stop matching rows here.
, exploded as (
    select
        feedback.feedback_pk
        , unioned.source_slug
        , {{ slugify('tag.tag_label') }}
            as tag_slug
    from unioned
    inner join feedback
        on unioned.source_record_ref = feedback.source_record_id
        and unioned.feedback_source_fk = feedback.feedback_source_fk
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

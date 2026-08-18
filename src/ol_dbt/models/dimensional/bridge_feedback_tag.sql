{{ config(
    materialized='table'
) }}

-- Tags are a ticket attribute applied to every turn of that ticket. That is deliberate:
-- a tag describes the conversation, and a per-turn bridge lets you filter turns by their
-- conversation's tags without joining back to the conversation fact (design 4e).
with unioned as (
    select
        source_slug
        , source_record_ref
        , source_tags
    from {{ ref('int__feedback__unioned') }}
    where source_tags is not null
)

, feedback_tag as (
    select * from {{ ref('dim_feedback_tag') }}
)

, exploded as (
    select
        {{ dbt_utils.generate_surrogate_key(['unioned.source_slug', 'unioned.source_record_ref']) }}
            as feedback_pk
        , unioned.source_slug
        , lower(regexp_replace(regexp_replace(tag.tag_label, '[^a-zA-Z0-9]+', '_'), '^_+|_+$', ''))
            as tag_slug
    from unioned
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

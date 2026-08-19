-- Seeded from Zendesk ticket tags plus group_name; LLM-labeled cluster rows upsert
-- alongside these later. Relabeling changes category_label, never category_slug.
with ticket as (
    select * from {{ ref('int__zendesk__ticket') }}
)

, feedback_tag as (
    select * from {{ ref('dim_feedback_tag') }}
)

, tag_seeds as (
    select
        feedback_tag.tag_slug as category_slug
        , feedback_tag.tag_label as category_label
        , min(ticket.ticket_created_at) as first_seen_at
        , max(ticket.ticket_updated_at) as updated_at
    from ticket
    cross join unnest(ticket.ticket_tags) as tag (tag_label)
    inner join feedback_tag
        on lower(regexp_replace(regexp_replace(tag.tag_label, '[^a-zA-Z0-9]+', '_'), '^_+|_+$', ''))
            = feedback_tag.tag_slug
        and feedback_tag.source_slug = 'zendesk'
    group by feedback_tag.tag_slug, feedback_tag.tag_label
)

, group_seeds as (
    select
        lower(regexp_replace(regexp_replace(ticket.group_name, '[^a-zA-Z0-9]+', '_'), '^_+|_+$', ''))
            as category_slug
        , min(ticket.group_name) as category_label
        , min(ticket.ticket_created_at) as first_seen_at
        , max(ticket.ticket_updated_at) as updated_at
    from ticket
    where ticket.group_name is not null
    group by 1
)

, combined as (
    select * from tag_seeds
    union all
    select * from group_seeds
)

-- a tag and a group name can slugify to the same value; collapse them so category_slug
-- stays unique
select
    {{ dbt_utils.generate_surrogate_key(['category_slug']) }} as feedback_category_pk
    , category_slug
    , min(category_label) as category_label
    , cast(null as varchar) as category_parent_slug
    , 'proposed' as category_status
    , 'seed' as category_source
    , cast(null as varchar) as cluster_run_id
    , min(first_seen_at) as first_seen_at
    , max(updated_at) as updated_at
from combined
where category_slug is not null
    and category_slug != ''
group by category_slug

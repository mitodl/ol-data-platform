-- Seeded from Zendesk ticket tags plus group_name; LLM-labeled cluster rows upsert
-- alongside these later. Relabeling changes category_label, never category_slug.
-- category_status defaults to 'proposed' until a human materializes the
-- feedback_category_approval Dagster asset for a slug -- that decision log is the
-- only place a category's status can change; dbt has no other approval input.
with ticket as (
    select
        *
        , {{ slugify('group_name') }} as group_slug
    from {{ ref('int__zendesk__ticket') }}
)

, feedback_tag as (
    select * from {{ ref('dim_feedback_tag') }}
)

, tag_seeds as (
    select
        feedback_tag.tag_slug as category_slug
        , feedback_tag.tag_label as category_label
        , 'seed' as category_source
        , cast(null as varchar) as cluster_run_id
        , min(ticket.ticket_created_at) as first_seen_at
        , max(ticket.ticket_updated_at) as updated_at
    from ticket
    cross join unnest(ticket.ticket_tags) as tag (tag_label)
    inner join feedback_tag
        on {{ slugify('tag.tag_label') }}
            = feedback_tag.tag_slug
        and feedback_tag.source_slug = 'zendesk'
    group by feedback_tag.tag_slug, feedback_tag.tag_label
)

, group_seeds as (
    select
        ticket.group_slug as category_slug
        , min(ticket.group_name) as category_label
        , 'seed' as category_source
        , cast(null as varchar) as cluster_run_id
        , min(ticket.ticket_created_at) as first_seen_at
        , max(ticket.ticket_updated_at) as updated_at
    from ticket
    where ticket.group_name is not null
    group by 1
)

-- One LLM call per cluster (ml.lib.categorize); always category_status='proposed'
-- until a human approves it below -- the assignment onto
-- afact_feedback_conversation only ever happens for an approved category.
, llm_proposed as (
    select
        category_slug
        , category_label
        , 'llm_discovered' as category_source
        , cluster_run_id
        -- varchar, matching tag_seeds/group_seeds' ticket_created_at/updated_at
        -- (ISO8601 strings throughout this layer, never a native timestamp).
        , {{ cast_timestamp_to_iso8601('proposed_at') }} as first_seen_at
        , {{ cast_timestamp_to_iso8601('proposed_at') }} as updated_at
    from {{ ref('int__feedback__category_proposal') }}
)

, combined as (
    select * from tag_seeds
    union all
    select * from group_seeds
    union all
    select * from llm_proposed
)

, approval as (
    select * from {{ ref('int__feedback__category_approval') }}
)

-- A tag/group name/LLM proposal can slugify to the same value; collapse them so
-- category_slug stays unique. category_source/cluster_run_id take max(): a seed
-- ('seed') sorts after an LLM proposal ('llm_discovered'), so a slug seeded from
-- existing structure keeps reading as 'seed' even if a cluster also proposed it;
-- cluster_run_id then reflects whichever row won that same comparison.
select
    {{ dbt_utils.generate_surrogate_key(['combined.category_slug']) }} as feedback_category_pk
    , combined.category_slug
    , min(combined.category_label) as category_label
    , cast(null as varchar) as category_parent_slug
    -- coalesce, not a bare default: a slug with no approval row is still 'proposed'
    , coalesce(min(approval.category_status), 'proposed') as category_status
    , max(combined.category_source) as category_source
    , max(combined.cluster_run_id) as cluster_run_id
    , min(combined.first_seen_at) as first_seen_at
    , max(combined.updated_at) as updated_at
from combined
left join approval
    on combined.category_slug = approval.category_slug
where combined.category_slug is not null
    and combined.category_slug != ''
group by combined.category_slug

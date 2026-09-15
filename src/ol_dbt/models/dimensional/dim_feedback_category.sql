-- Seeded from Zendesk ticket tags plus group_name. Relabeling changes category_label,
-- never category_slug. category_status is always 'proposed' -- no approval input yet.
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

-- A tag name and its group name can slugify to the same value; collapse them so
-- category_slug stays unique. category_label/category_source must come from the
-- *same* row, not independent min()/max() picks across rows. Ties break on the
-- most recently updated row.
, ranked_combined as (
    select
        *
        , row_number() over (
            partition by category_slug
            order by updated_at desc
        ) as category_rank
    from combined
    where category_slug is not null
        and category_slug != ''
)

, slug_dates as (
    select
        category_slug
        , min(first_seen_at) as first_seen_at
        , max(updated_at) as updated_at
    from combined
    where category_slug is not null
        and category_slug != ''
    group by category_slug
)

select
    {{ dbt_utils.generate_surrogate_key(['ranked_combined.category_slug']) }}
        as feedback_category_pk
    , ranked_combined.category_slug
    , ranked_combined.category_label
    , cast(null as varchar) as category_parent_slug
    , 'proposed' as category_status
    , ranked_combined.category_source
    , slug_dates.first_seen_at
    , slug_dates.updated_at
from ranked_combined
inner join slug_dates
    on ranked_combined.category_slug = slug_dates.category_slug
where ranked_combined.category_rank = 1

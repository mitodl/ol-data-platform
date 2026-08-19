-- Tags are source-scoped, not conformed: a Zendesk tag and a forum role that share a
-- string are not the same thing.
with ticket as (
    select * from {{ ref('int__zendesk__ticket') }}
)

, zendesk_tags as (
    select
        'zendesk' as source_slug
        , tag.tag_label
        , lower(regexp_replace(regexp_replace(tag.tag_label, '[^a-zA-Z0-9]+', '_'), '^_+|_+$', '')) as tag_slug
    from ticket
    cross join unnest(ticket.ticket_tags) as tag (tag_label)
    where tag.tag_label is not null
        and tag.tag_label != ''
)

-- grouped on the slug, not the label: two labels that slugify identically are one tag,
-- and the dimension must stay unique on (source_slug, tag_slug)
select
    {{ dbt_utils.generate_surrogate_key(['source_slug', 'tag_slug']) }} as feedback_tag_pk
    , tag_slug
    , min(tag_label) as tag_label
    , source_slug
from zendesk_tags
where tag_slug != ''
group by source_slug, tag_slug

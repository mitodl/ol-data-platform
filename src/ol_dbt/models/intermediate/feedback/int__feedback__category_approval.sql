-- The latest human decision per category_slug -- dim_feedback_category coalesces
-- to 'proposed' for any slug missing here. A category has no row until a human
-- materializes the feedback_category_approval asset (via its Launchpad Config
-- form: category_slug, category_status, approved_by).
{% set approval_source = source('feedback_intermediate', 'feedback_category_approval') %}
{% set approval_relation_exists = adapter.get_relation(
    database=approval_source.database,
    schema=approval_source.schema,
    identifier=approval_source.identifier
) %}

{% if execute and not approval_relation_exists %}
select
    cast(null as varchar) as category_slug
    , cast(null as varchar) as category_status
where false
{% else %}
with ranked_approval as (
    select
        category_slug
        , category_status
        , row_number() over (
            partition by category_slug order by approved_at desc
        ) as decision_rank
    from {{ approval_source }}
)

select
    category_slug
    , category_status
from ranked_approval
where decision_rank = 1
{% endif %}

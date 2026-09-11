-- Every LLM-proposed category, from every clustering run that has ever been
-- labeled -- not scoped to the promoted run. A proposal is only usable once a
-- human approves it via feedback_category_approval (dim_feedback_category
-- defaults category_status to 'proposed' otherwise), so surfacing proposals
-- from a superseded run doesn't risk assigning anything prematurely.
{% set proposal_source = source('feedback_intermediate', 'feedback_category_proposal') %}
{% set proposal_relation_exists = adapter.get_relation(
    database=proposal_source.database,
    schema=proposal_source.schema,
    identifier=proposal_source.identifier
) %}

{% if execute and not proposal_relation_exists %}
-- feedback_category_proposals has never materialized in this schema -- an
-- empty, correctly typed stub so dim_feedback_category can still build.
select
    cast(null as varchar) as cluster_run_id
    , cast(null as integer) as cluster_id
    , cast(null as varchar) as category_slug
    , cast(null as varchar) as category_label
    , cast(null as timestamp) as proposed_at
where false
{% else %}
select
    cluster_run_id
    , cluster_id
    , category_slug
    , category_label
    , proposed_at
from {{ proposal_source }}
{% endif %}

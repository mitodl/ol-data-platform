-- Every LLM-proposed category, one row per cluster_key that has ever been
-- labeled -- not scoped to any particular run. A continued cluster_key keeps its
-- existing row (feedback_category_proposals only writes for new/split/merged
-- keys); dim_feedback_category unions this in, always category_status='proposed'
-- until a human corrects it.
{% set proposal = dev_schema_source('feedback_intermediate', 'feedback_category_proposal') %}
{% set cluster = dev_schema_source('feedback_intermediate', 'feedback_cluster') %}
{% set has_cluster = cluster.is_unit_test or not execute or cluster.resolved_relation %}

{% if not proposal.is_unit_test and execute and not proposal.resolved_relation %}
select
    cast(null as varchar) as cluster_key
    , cast(null as varchar) as cluster_run_id
    , cast(null as varchar) as category_slug
    , cast(null as varchar) as category_label
    , cast(null as varchar) as category_description
    , cast(null as timestamp) as proposed_at
    , cast(null as varchar) as cluster_status
where false
{% else %}
select
    proposal.cluster_key
    , proposal.cluster_run_id
    , proposal.category_slug
    , proposal.category_label
    , proposal.category_description
    , proposal.proposed_at
    , {{ 'cluster.cluster_status' if has_cluster else 'cast(null as varchar)' }} as cluster_status
from {{ proposal.relation_ref }} as proposal
{% if has_cluster %}
left join {{ cluster.relation_ref }} as cluster
    on proposal.cluster_key = cluster.cluster_key
{% endif %}
{% endif %}

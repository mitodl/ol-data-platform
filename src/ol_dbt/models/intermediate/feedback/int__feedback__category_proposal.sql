-- Every LLM-proposed category, one row per cluster_key that has ever been
-- labeled -- not scoped to any particular run. A continued cluster_key keeps its
-- existing row (feedback_category_proposals only writes for new/split/merged
-- keys); dim_feedback_category unions this in, always category_status='proposed'
-- until a human corrects it.
{% set proposal = dev_schema_source('feedback_intermediate', 'feedback_category_proposal') %}

{% if not proposal.is_unit_test and execute and not proposal.resolved_relation %}
select
    cast(null as varchar) as cluster_key
    , cast(null as varchar) as cluster_run_id
    , cast(null as varchar) as category_slug
    , cast(null as varchar) as category_label
    , cast(null as varchar) as category_description
    , cast(null as timestamp) as proposed_at
where false
{% else %}
select
    cluster_key
    , cluster_run_id
    , category_slug
    , category_label
    , category_description
    , proposed_at
from {{ proposal.relation_ref }}
{% endif %}

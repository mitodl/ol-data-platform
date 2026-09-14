-- Every LLM-proposed category, one row per cluster_key that has ever been
-- labeled -- not scoped to any particular run. A continued cluster_key keeps its
-- existing row (feedback_category_proposals only writes for new/split/merged
-- keys); dim_feedback_category unions this in, always category_status='proposed'
-- until a human corrects it.
{% set proposal = dev_schema_source('feedback_intermediate', 'feedback_category_proposal') %}
{# cluster_key replaces the old cluster_run_id/cluster_id compound key -- a table
   written before this rework won't have it until the Dagster asset's own
   schema-evolution step next runs. #}
{% set proposal_columns = adapter.get_columns_in_relation(proposal.resolved_relation) | map(attribute='name') | list
    if proposal.resolved_relation else [] %}
{% set cluster_key_exists = 'cluster_key' in proposal_columns %}
{% set category_description_exists = 'category_description' in proposal_columns %}

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
    {{ 'cluster_key' if cluster_key_exists else 'cast(null as varchar)' }} as cluster_key
    , cluster_run_id
    , category_slug
    , category_label
    , {{ 'category_description' if category_description_exists else 'cast(null as varchar)' }} as category_description
    , proposed_at
from {{ proposal.relation_ref }}
{% endif %}

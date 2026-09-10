-- feedback_summaries' output, one row per conversation (its own upsert key is
-- feedback_conversation_pk) -- empty until the feedback_summaries asset has
-- materialized in this schema. conversation_summary/summary_model_version stay
-- null for a single-turn/short conversation regardless (the asset's own skip
-- rule, §A.1), not just when the table is missing.
{% set summary_source = source('feedback_intermediate', 'feedback_summaries') %}
{% set summary_relation = adapter.get_relation(
    database=summary_source.database,
    schema=summary_source.schema,
    identifier=summary_source.identifier
) %}
{# summarized_at is a newer column -- a table upserted before it existed won't
   have it until the Dagster asset's own schema-evolution step next runs. #}
{% set summarized_at_exists = summary_relation and 'summarized_at' in (
    adapter.get_columns_in_relation(summary_relation) | map(attribute='name') | list
) %}

{% if execute and not summary_relation %}
select
    cast(null as varchar) as feedback_conversation_pk
    , cast(null as varchar) as conversation_summary
    , cast(null as varchar) as summary_model_version
    , cast(null as timestamp) as summarized_at
where false
{% else %}
select
    feedback_conversation_pk
    , conversation_summary
    , summary_model_version
    , {{ 'summarized_at' if summarized_at_exists else 'cast(null as timestamp)' }} as summarized_at
from {{ summary_source }}
{% endif %}

-- feedback_embeddings' output, one row per conversation (its own upsert key is
-- feedback_conversation_pk, so a re-embed overwrites in place -- there is never
-- more than one arm/model live per conversation at once) -- empty until the
-- feedback_embeddings asset has materialized in this schema.
{% set embedding = resolve_dev_source_with_fallback('feedback_intermediate', 'feedback_embeddings') %}
{% set embedding_relation = embedding.primary_relation or embedding.fallback_relation %}
{# embedded_at is a newer column -- a table upserted before it existed won't
   have it until the Dagster asset's own schema-evolution step next runs. #}
{% set embedded_at_exists = embedding_relation and 'embedded_at' in (
    adapter.get_columns_in_relation(embedding_relation) | map(attribute='name') | list
) %}

{% if not embedding.is_unit_test and execute and not embedding_relation %}
select
    cast(null as varchar) as feedback_conversation_pk
    , {{ null_double_array() }} as embedding_vector
    , cast(null as integer) as embedding_dim
    , cast(null as varchar) as embedding_model_version
    , cast(null as varchar) as embedding_input
    , cast(null as varchar) as embedded_at
where false
{% else %}
select
    feedback_conversation_pk
    , {{ cast_double_array('embedding_vector') }} as embedding_vector
    , embedding_dim
    , embedding_model_version
    , embedding_input
    -- This layer stores timestamps as ISO8601 varchar; embedded_at arrives as a
    -- native timestamp from Iceberg, so it needs converting, not a passthrough.
    , {{ cast_timestamp_to_iso8601('embedded_at') if embedded_at_exists else 'cast(null as varchar)' }} as embedded_at
from {{ embedding.fallback_relation if (not embedding.is_unit_test and not embedding.primary_relation and embedding.fallback_relation) else embedding.source_ref }}
{% endif %}

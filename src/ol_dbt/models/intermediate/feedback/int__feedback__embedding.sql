-- feedback_embeddings' output, one row per conversation (its own upsert key is
-- feedback_conversation_pk, so a re-embed overwrites in place -- there is never
-- more than one arm/model live per conversation at once) -- empty until the
-- feedback_embeddings asset has materialized in this schema.
{% set embedding_source = source('feedback_intermediate', 'feedback_embeddings') %}
{% set embedding_relation = adapter.get_relation(
    database=embedding_source.database,
    schema=embedding_source.schema,
    identifier=embedding_source.identifier
) %}
{# embedded_at is a newer column -- a table upserted before it existed won't
   have it until the Dagster asset's own schema-evolution step next runs. #}
{% set embedded_at_exists = embedding_relation and 'embedded_at' in (
    adapter.get_columns_in_relation(embedding_relation) | map(attribute='name') | list
) %}

{% if execute and not embedding_relation %}
select
    cast(null as varchar) as feedback_conversation_pk
    , {{ null_double_array() }} as embedding_vector
    , cast(null as integer) as embedding_dim
    , cast(null as varchar) as embedding_model_version
    , cast(null as varchar) as embedding_input
    , cast(null as timestamp) as embedded_at
where false
{% else %}
select
    feedback_conversation_pk
    , cast(embedding_vector as array(double)) as embedding_vector
    , embedding_dim
    , embedding_model_version
    , embedding_input
    , {{ 'embedded_at' if embedded_at_exists else 'cast(null as timestamp)' }} as embedded_at
from {{ embedding_source }}
{% endif %}

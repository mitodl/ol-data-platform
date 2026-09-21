-- A conversation can have one row per model/dim now; this picks the most
-- recent so downstream still gets one row per conversation. Empty until the
-- feedback_embeddings asset has materialized in this schema.
{% set embedding = dev_schema_source('feedback_intermediate', 'feedback_embeddings') %}

{% if not embedding.is_unit_test and execute and not embedding.resolved_relation %}
select
    cast(null as varchar) as feedback_conversation_pk
    , {{ null_double_array() }} as embedding_vector
    , cast(null as integer) as embedding_dim
    , cast(null as varchar) as embedding_model_version
    , cast(null as varchar) as embedding_input
    , cast(null as varchar) as embedded_at
where false
{% else %}
with ranked as (
    select
        feedback_conversation_pk
        , {{ cast_double_array('embedding_vector') }} as embedding_vector
        , embedding_dim
        , embedding_model_version
        , embedding_input
        -- This layer stores timestamps as ISO8601 varchar; embedded_at arrives
        -- as a native timestamp from Iceberg, so it needs converting, not a
        -- passthrough.
        , {{ cast_timestamp_to_iso8601('embedded_at') }} as embedded_at
        , row_number() over (
            partition by feedback_conversation_pk
            order by embedded_at desc
        ) as recency_rank
    from {{ embedding.relation_ref }}
)
select
    feedback_conversation_pk
    , embedding_vector
    , embedding_dim
    , embedding_model_version
    , embedding_input
    , embedded_at
from ranked
where recency_rank = 1
{% endif %}

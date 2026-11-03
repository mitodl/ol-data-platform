-- feedback_sentiment_assignments' output, one row per conversation the model scored. Empty until the feedback_sentiment asset has materialized in this schema.
{% set sentiment = dev_schema_source('feedback_intermediate', 'feedback_sentiment_assignments') %}

{% if not sentiment.is_unit_test and execute and not sentiment.resolved_relation %}
select
    cast(null as varchar) as feedback_conversation_pk
    , cast(null as varchar) as sentiment_fk
    , cast(null as varchar) as sentiment_slug
    , cast(null as double) as negative_probability
    , cast(null as varchar) as sentiment_model_version
    , cast(null as varchar) as embedding_model_version
    , cast(null as varchar) as predicted_at
where false
{% else %}
select
    feedback_conversation_pk
    , {{ dbt_utils.generate_surrogate_key(['sentiment_slug']) }} as sentiment_fk
    , sentiment_slug
    , negative_probability
    , sentiment_model_version
    , embedding_model_version
    , {{ cast_timestamp_to_iso8601('predicted_at') }} as predicted_at
from {{ sentiment.relation_ref }}
{% endif %}

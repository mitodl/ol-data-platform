-- feedback_cluster_membership's output, one row per conversation -- the live
-- cluster assignment (ml.assets.feedback_cluster_assignment). Empty until that
-- asset has materialized in this schema.
{% set membership = dev_schema_source('feedback_intermediate', 'feedback_cluster_membership') %}

{% if not membership.is_unit_test and execute and not membership.resolved_relation %}
select
    cast(null as varchar) as feedback_conversation_pk
    , cast(null as varchar) as cluster_key
    , cast(null as double) as cluster_similarity
    , cast(null as varchar) as cluster_assignment_method
    , cast(null as varchar) as cluster_run_id
    , cast(null as varchar) as assigned_at
where false
{% else %}
select
    feedback_conversation_pk
    , cluster_key
    , cluster_similarity
    , cluster_assignment_method
    , cluster_run_id
    -- This layer stores timestamps as ISO8601 varchar; assigned_at arrives as a
    -- native timestamp from Iceberg, so it needs converting, not a passthrough.
    , {{ cast_timestamp_to_iso8601('assigned_at') }} as assigned_at
from {{ membership.relation_ref }}
{% endif %}

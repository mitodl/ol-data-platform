-- The promoted clustering run's per-conversation assignments -- the only
-- feedback_cluster_candidate rows afact_feedback_conversation is allowed to see.
-- A run stays invisible to the fact until a human materializes the
-- feedback_cluster_run_promotion asset with run_status='promoted' for its
-- cluster_run_id (via that asset's Launchpad Config form). The most recently
-- promoted decision wins -- a newer promotion supersedes an older one without
-- needing an explicit 'superseded' row.
{% set candidate_source = source('feedback_intermediate', 'feedback_cluster_candidate') %}
{% set promotion_source = source('feedback_intermediate', 'feedback_cluster_run_promotion') %}
{% set candidate_relation_exists = adapter.get_relation(
    database=candidate_source.database,
    schema=candidate_source.schema,
    identifier=candidate_source.identifier
) %}
{% set promotion_relation_exists = adapter.get_relation(
    database=promotion_source.database,
    schema=promotion_source.schema,
    identifier=promotion_source.identifier
) %}

{% if execute and (not candidate_relation_exists or not promotion_relation_exists) %}
-- Neither asset has materialized in this schema yet -- an empty, correctly typed
-- stub so afact_feedback_conversation can still build with all-null cluster
-- columns, rather than a hard failure on a table that legitimately doesn't exist.
select
    cast(null as varchar) as feedback_conversation_pk
    , cast(null as varchar) as cluster_run_id
    , cast(null as integer) as cluster_id
    , cast(null as double) as cluster_probability
where false
{% else %}
-- Rank every decision (any status) per run before filtering, not the other way
-- around -- filtering to 'promoted' first would let an older promoted row keep
-- winning even after a later 'superseded' row for that same run, since the
-- superseded row (having no 'promoted' status) would simply be discarded rather
-- than counted as that run's current, deactivating state.
with latest_decision_per_run as (
    select
        cluster_run_id
        , run_status
        , promoted_at
        , row_number() over (
            partition by cluster_run_id order by promoted_at desc
        ) as run_decision_rank
    from {{ promotion_source }}
)

, active_run as (
    select cluster_run_id
    from latest_decision_per_run
    where run_decision_rank = 1 and run_status = 'promoted'
    order by promoted_at desc
    limit 1
)

select
    cluster_candidate.feedback_conversation_pk
    , cluster_candidate.cluster_run_id
    , cluster_candidate.cluster_id
    , cluster_candidate.cluster_probability
from {{ candidate_source }} as cluster_candidate
inner join active_run
    on cluster_candidate.cluster_run_id = active_run.cluster_run_id
{% endif %}

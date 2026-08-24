-- THE DURABLE user_pk. Append-only: an account natural key is assigned a person key once
-- and never reassigned. See docs/design/adr_durable_user_surrogate_key.md.
--
-- Why this exists: dim_user used to compute user_pk as a first_value() over
-- `partition by email` on a table-materialized model, so the key was an attribute
-- recomputed every build, not a key. Four routine events re-keyed a person (a
-- higher-ranked account joining the email group, a user_global_id appearing, an email
-- edit, an activity-flag toggle), orphaning user_fk across 27 downstream models and every
-- reverse-ETL consumer holding the value.
--
-- `full_refresh=false` is load-bearing. dbt-core's should_full_refresh() reads the model
-- config BEFORE the CLI flag, so `dbt build --full-refresh` is a no-op here specifically.
-- Without it, one --full-refresh re-keys the warehouse. Do not remove it to "clean up" the
-- table: this model is state, and the assignment ORDER it holds cannot be recovered from
-- the sources.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    full_refresh=false,
    on_schema_change='append_new_columns',
    meta={
        'iceberg_maintenance': {
            'enabled': true,
            'snapshot_retention_days': 30,
            'orphan_retention_days': 30,
            'optimize_after_every_n_runs': 1,
            'analyze_after_every_n_runs': 7
        }
    }
) }}

-- Ranking identical to dim_user's, and it must stay identical: it decides which account a
-- NEW group mints its key from, and dim_user independently uses it to decide which
-- account's attributes surface. Both call the same macros so they cannot drift.
with ranked_accounts as (
    select
        {{ user_account_rank_columns() }}
        , {{ user_account_nk() }} as account_nk
        , accounts.*
    from {{ ref('int__combined__user_accounts') }} as accounts
)

-- The winner's identity, per email group. This reproduces dim_user's account_identity
-- exactly, because the minted key below must equal the key dim_user produces today --
-- that is what makes cutover a no-op.
, account_identity as (
    select
        first_value(case
            when user_global_id is not null then 'global'
            when id_source_user_id is not null then id_source
            else 'email'
        end) over w as winner_identity_source
        , first_value(coalesce(user_global_id, id_source_user_id, email))
            over w as winner_identity_id
        , ranked_accounts.*
    from ranked_accounts
    window w as (
        partition by email
        order by {{ user_account_rank_order() }}
    )
)

-- account_nk is unique for id-bearing rows but not for the id-less ones, which key on
-- email: two Emeritus rows can share an address. Collapsing them here keeps the map one
-- row per key, so the join in dim_user cannot fan out.
, accounts_deduped as (
    select *
    from (
        select
            *
            , row_number() over (
                partition by account_nk
                order by has_no_source_id, id_source_rank, sort_id desc nulls last
            ) as account_row_num
        from account_identity
    )
    where account_row_num = 1
)

{% if is_incremental() %}

, existing_map as (
    select
        account_nk
        , user_pk
        , assigned_at
    from {{ this }}
)

-- The group's incumbent key: whichever of its already-mapped accounts was assigned first.
-- Survivorship is decided by ASSIGNMENT ORDER, which is immutable, rather than by the
-- platform ranking, which is exactly what used to move underneath the key.
, group_incumbent as (
    select
        email
        , user_pk
    from (
        select
            accounts_deduped.email
            , existing_map.user_pk
            , row_number() over (
                partition by accounts_deduped.email
                order by existing_map.assigned_at, existing_map.user_pk
            ) as incumbent_row_num
        from accounts_deduped
        inner join existing_map on accounts_deduped.account_nk = existing_map.account_nk
    )
    where incumbent_row_num = 1
)

, unmapped_accounts as (
    select accounts_deduped.*
    from accounts_deduped
    left join existing_map on accounts_deduped.account_nk = existing_map.account_nk
    where existing_map.account_nk is null
)

select
    unmapped_accounts.account_nk
    -- Rule 1: adopt the group's incumbent if it has one. Rule 2: mint. A group that gains
    -- a member adopts; only a genuinely new group mints.
    , coalesce(
        group_incumbent.user_pk
        , {{ dbt_utils.generate_surrogate_key([
            'unmapped_accounts.winner_identity_source',
            'unmapped_accounts.winner_identity_id'
        ]) }}
    ) as user_pk
    , {{ cast_timestamp_to_iso8601('current_timestamp') }} as assigned_at
    , '{{ invocation_id }}' as assigned_invocation_id
from unmapped_accounts
left join group_incumbent on unmapped_accounts.email = group_incumbent.email

{% else %}

-- First build: the map is empty, so every group mints. The mint expression is the one
-- dim_user used before this model existed, so every person's user_pk on cutover day is
-- the value they already have. No downstream re-key, no reverse-ETL coordination.
select
    account_nk
    , {{ dbt_utils.generate_surrogate_key([
        'winner_identity_source',
        'winner_identity_id'
    ]) }} as user_pk
    , {{ cast_timestamp_to_iso8601('current_timestamp') }} as assigned_at
    , '{{ invocation_id }}' as assigned_invocation_id
from accounts_deduped

{% endif %}

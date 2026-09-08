-- THE DURABLE user_pk. Append-only: an identifier is assigned a person key once and never
-- reassigned. See docs/design/adr_durable_user_surrogate_key.md.
--
-- Why this exists: dim_user used to compute user_pk as a first_value() over
-- `partition by email` on a table-materialized model, so the key was an attribute
-- recomputed every build, not a key. Four routine events re-keyed a person (a
-- higher-ranked account joining the email group, a user_global_id appearing, an email
-- edit, an activity-flag toggle), orphaning user_fk across 27 downstream models and every
-- reverse-ETL consumer holding the value.
--
-- The grain is one row per IDENTIFIER, not per account. An account holding several
-- platform ids contributes one row per id, all carrying the same user_pk, so a newly
-- appearing id resolves through the account's existing identifiers and ADOPTS its key
-- instead of minting a new one. Keying on any single id instead would re-key ~795k people
-- over the MIT Learn rollout; see user_account_identifier_rows() for the measurements.
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

-- The ranking decides which account a NEW group mints its key from. dim_user applies the
-- identical ranking (same macros) to decide which account's attributes surface.
with ranked_accounts as (
    select
        {{ user_account_rank_columns() }}
        , {{ user_account_nk() }} as account_nk
        , accounts.*
    from {{ ref('int__combined__user_accounts') }} as accounts
)

-- The winner's identity, per email group. This reproduces the expression dim_user used
-- before the map existed, which is what makes cutover a no-op.
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

-- winner_identity_* is a first_value over the whole email group, so it is constant within
-- an email and one row per email is enough to mint from.
, group_winner as (
    select distinct
        email
        , winner_identity_source
        , winner_identity_id
    from account_identity
)

, identifiers as (
    {{ user_account_identifier_rows('account_identity') }}
)

-- Identifiers are unique across account rows for every id-bearing namespace, but the
-- email fallback is not: two id-less Emeritus rows can share an address. Collapsing keeps
-- the map one row per identifier, so the join in dim_user cannot fan out.
, identifiers_deduped as (
    select
        account_nk
        , email
        , identifier
    from (
        select
            identifiers.*
            , row_number() over (
                partition by identifier order by account_nk
            ) as identifier_row_num
        from identifiers
    )
    where identifier_row_num = 1
)

{% if is_incremental() %}

, existing_map as (
    select
        identifier
        , user_pk
        , user_pk_source
        , assigned_at
    from {{ this }}
)

-- The person's incumbent key: the oldest key reachable from ANY identifier anywhere in
-- their email group. Reaching through the whole identifier set is what makes a newly
-- appearing id adopt rather than mint. Survivorship is decided by ASSIGNMENT ORDER, which
-- is immutable, rather than by the platform ranking, which is what used to move underneath
-- the key.
, group_incumbent as (
    select
        email
        , user_pk
        , user_pk_source
    from (
        select
            identifiers_deduped.email
            , existing_map.user_pk
            , existing_map.user_pk_source
            , row_number() over (
                partition by identifiers_deduped.email
                order by existing_map.assigned_at, existing_map.user_pk
            ) as incumbent_row_num
        from identifiers_deduped
        inner join existing_map
            on identifiers_deduped.identifier = existing_map.identifier
    )
    where incumbent_row_num = 1
)

, unmapped_identifiers as (
    select identifiers_deduped.*
    from identifiers_deduped
    left join existing_map
        on identifiers_deduped.identifier = existing_map.identifier
    where existing_map.identifier is null
)

select
    unmapped_identifiers.identifier
    -- Rule 1: adopt the group's incumbent if it has one -- which now includes the case of
    -- an account acquiring a new id, because its older identifiers are still mapped.
    -- Rule 2: mint. Only a genuinely new person mints.
    , coalesce(
        group_incumbent.user_pk
        , {{ dbt_utils.generate_surrogate_key([
            'group_winner.winner_identity_source',
            'group_winner.winner_identity_id'
        ]) }}
    ) as user_pk
    -- Travels WITH the key, not recomputed. dim_user reports this rather than the current
    -- ranking winner, which would otherwise disagree with the key it is describing.
    , coalesce(
        group_incumbent.user_pk_source
        , group_winner.winner_identity_source
    ) as user_pk_source
    , {{ cast_timestamp_to_iso8601('current_timestamp') }} as assigned_at
    , '{{ invocation_id }}' as assigned_invocation_id
from unmapped_identifiers
left join group_incumbent on unmapped_identifiers.email = group_incumbent.email
inner join group_winner on unmapped_identifiers.email = group_winner.email

{% else %}

-- First build: the map is empty, so every group mints. The mint expression is the one
-- dim_user used before this model existed, so every person's user_pk on cutover day is
-- the value they already have. No downstream re-key, no reverse-ETL coordination.
select
    identifiers_deduped.identifier
    , {{ dbt_utils.generate_surrogate_key([
        'group_winner.winner_identity_source',
        'group_winner.winner_identity_id'
    ]) }} as user_pk
    , group_winner.winner_identity_source as user_pk_source
    , {{ cast_timestamp_to_iso8601('current_timestamp') }} as assigned_at
    , '{{ invocation_id }}' as assigned_invocation_id
from identifiers_deduped
inner join group_winner on identifiers_deduped.email = group_winner.email

{% endif %}

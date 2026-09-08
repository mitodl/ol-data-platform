-- Retired-to-surviving user_pk map, for the case identity resolution actually needs: two
-- people who turn out to be one. When a user_global_id appears and links a mitxpro account
-- to a mitxonline account, those accounts keep their own assigned keys (the map never
-- reassigns), so the person now has more than one. dim_user reports the survivor; this
-- table names the keys it retired, so a downstream FK pointing at one is remapped
-- deliberately instead of silently orphaning.
--
-- Derived every run, not persisted. That is deliberate: because it is recomputed from the
-- current grouping, every non-survivor points DIRECTLY at the one survivor, so alias
-- chains cannot form and no recursive resolution is needed. Persisting merge decisions
-- would reintroduce chains.
--
-- Empty is the normal state. A non-empty result is a real merge event and reverse-ETL
-- consumers holding user_pk need to act on it.
{{ config(materialized='table') }}

with identifiers as (
    {{ user_account_identifier_rows(ref('int__combined__user_accounts')) }}
)

, account_keys as (
    select
        identifiers.email
        , user_key_map.user_pk
        , user_key_map.assigned_at
    from identifiers
    inner join {{ ref('int__combined__user_key_map') }} as user_key_map
        on identifiers.identifier = user_key_map.identifier
)

-- Same survivorship rule as dim_user: oldest assignment wins.
, group_survivor as (
    select
        email
        , user_pk
    from (
        select
            email
            , user_pk
            , row_number() over (
                partition by email
                order by assigned_at, user_pk
            ) as survivor_row_num
        from account_keys
    )
    where survivor_row_num = 1
)

-- A key is only RETIRED if it survives nowhere. The same key can be a non-survivor in one
-- email group and the survivor of another -- two accounts once shared it, and one of them
-- moved to a group with an older incumbent. Publishing that as retired would tell
-- consumers to remap a key that is still the live identity of a different person, so the
-- `not exists` is load-bearing rather than defensive.
, live_keys as (
    select distinct user_pk from group_survivor
)

select distinct
    account_keys.user_pk as retired_user_pk
    , group_survivor.user_pk as surviving_user_pk
from account_keys
inner join group_survivor on account_keys.email = group_survivor.email
where account_keys.user_pk != group_survivor.user_pk
    and not exists (
        select 1 from live_keys where live_keys.user_pk = account_keys.user_pk
    )

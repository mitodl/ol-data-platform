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

with account_keys as (
    select
        accounts.email
        , user_key_map.user_pk
        , user_key_map.assigned_at
    from {{ ref('int__combined__user_accounts') }} as accounts
    inner join {{ ref('int__combined__user_key_map') }} as user_key_map
        on {{ user_account_nk() }} = user_key_map.account_nk
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

select distinct
    account_keys.user_pk as retired_user_pk
    , group_survivor.user_pk as surviving_user_pk
from account_keys
inner join group_survivor on account_keys.email = group_survivor.email
where account_keys.user_pk != group_survivor.user_pk

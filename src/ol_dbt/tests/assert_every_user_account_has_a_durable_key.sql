-- Every account must resolve to a key through at least one of its identifiers.
--
-- dim_user's `nulls last` survivor pick deliberately tolerates a PARTIALLY adopted group:
-- an account whose identifiers are not yet mapped, sitting alongside one that is, takes the
-- mapped key, which is the correct mid-run state. That tolerance means a genuinely unmapped
-- account does not surface as a null user_pk when it shares an email with a mapped one, so
-- the not_null test on dim_user.user_pk cannot be the thing that catches it.
--
-- error_if '>0' rather than the project default of '>10': an unmapped account is silently
-- inheriting a key that was never assigned to it, and one is as wrong as eleven.
{{ config(error_if = '>0') }}

with identifiers as (
    {{ user_account_identifier_rows(ref('int__combined__user_accounts')) }}
)

select
    identifiers.account_nk
    , count(*) as identifiers_held
    , count(user_key_map.identifier) as identifiers_mapped
from identifiers
left join {{ ref('int__combined__user_key_map') }} as user_key_map
    on identifiers.identifier = user_key_map.identifier
group by identifiers.account_nk
having count(user_key_map.identifier) = 0

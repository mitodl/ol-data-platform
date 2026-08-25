{#
    Identity-resolution expressions shared by int__combined__user_key_map, dim_user and
    bridge_user_key_alias. They exist as macros because all three must agree exactly:
    account_nk is the join key between the map and its consumers, and the ranking decides
    which account a new group mints its key from AND which account's attributes dim_user
    reports. Three literal copies would drift, and the drift would be silent.

    Both expect the column names of int__combined__user_accounts: id_source,
    id_source_user_id, user_global_id, email.
#}

{#
    A WITHIN-RUN row identifier for an account. Priority-picked, so it is NOT stable across
    runs -- an account that gains a higher-priority id changes it. That is fine here and
    only here: nothing durable keys on it. Durability lives in
    user_account_identifier_rows() below. Do not reintroduce this as the key map's key.
#}
{% macro user_account_nk() %}
case
    when id_source_user_id is not null then id_source || ':' || id_source_user_id
    else 'email:' || email
end
{% endmacro %}

{#
    EVERY identifier an account carries, one row each -- the durable lookup the key map is
    built on.

    Why a set rather than one key. A combined MITx/Learn row holds several platform ids at
    once, and any single-key scheme has to pick one of them by priority. Whichever it picks
    moves the moment a higher-priority id appears: measured on 7.68M production accounts,
    keying on (id_source, id_source_user_id) would re-key 795,312 people over the MIT Learn
    rollout, and keying on coalesce(user_global_id, ...) 796,593 -- the global id and the
    mitlearn id arrive in the same event, so it relocates the flip rather than fixing it.

    Keying on the whole set removes the choice. A new id is an ADDITIONAL identifier, so the
    account's earlier identifiers still resolve to its existing key and the map adopts
    rather than mints. Verified unique: 9,718,753 identifiers across the snapshot, zero
    collisions between account rows.

    `source` is a relation carrying int__combined__user_accounts' columns. Emits
    (account_nk, email, identifier).
#}
{% macro user_account_identifier_rows(source) %}
{%- set namespaces = [
    ('global', 'user_global_id'),
    ('mitlearn', 'mitlearn_user_id'),
    ('mitlearn_openedx', 'mitlearn_openedx_user_id'),
    ('mitxonline', 'mitxonline_application_user_id'),
    ('mitxonline_openedx', 'mitxonline_openedx_user_id'),
    ('edxorg', 'edxorg_openedx_user_id'),
    ('micromasters', 'micromasters_user_id'),
    ('mitxpro', 'mitxpro_application_user_id'),
    ('mitxpro_openedx', 'mitxpro_openedx_user_id'),
    ('residential', 'residential_openedx_user_id'),
    ('bootcamps', 'bootcamps_application_user_id'),
    ('emeritus', 'emeritus_user_id'),
    ('global_alumni', 'global_alumni_user_id')
] -%}
{% for namespace, column in namespaces %}
select
    {{ user_account_nk() }} as account_nk
    , email
    , '{{ namespace }}:' || cast({{ column }} as varchar) as identifier
from {{ source }}
where {{ column }} is not null
union all
{% endfor %}
-- Accounts with no source id anywhere (Emeritus and Global Alumni rows that pre-date
-- stable ids) have nothing but their email. Still not durable across an email edit, and
-- nothing else identifies them.
select
    {{ user_account_nk() }} as account_nk
    , email
    , 'email:' || email as identifier
from {{ source }}
where id_source_user_id is null and user_global_id is null
{% endmacro %}

{#
    Ranks the accounts sharing an email, best first. Emits three columns:
    has_no_source_id, id_source_rank, sort_id.
#}
{% macro user_account_rank_columns() %}
-- Outranks id_source_rank: an id-less emeritus row (9) must still lose to an id-bearing
-- global_alumni row (10).
case when id_source_user_id is null then 1 else 0 end as has_no_source_id
, case
    when user_global_id is not null then 0
    when id_source = 'mitlearn' then 1
    when id_source = 'mitxonline' then 2
    when id_source = 'edxorg' then 3
    when id_source = 'micromasters' then 4
    when id_source = 'mitxonline_openedx' then 5
    when id_source = 'mitxpro' then 6
    when id_source = 'residential' then 7
    when id_source = 'bootcamps' then 8
    when id_source = 'emeritus' then 9
    when id_source = 'global_alumni' then 10
end as id_source_rank
-- Emeritus and Global Alumni ids are varchar, and dim_user's agg_view surfaces them with a
-- lexicographic max(). Nulling sort_id falls the ordering through to the lexicographic key
-- so the reported source names the account those ids come from.
, case
    when id_source in ('emeritus', 'global_alumni') then null
    else try_cast(id_source_user_id as bigint)
end as sort_id
{% endmacro %}

{#
    The window ordering that pairs with user_account_rank_columns(). Used as the ORDER BY
    of a `partition by email` window.
#}
{% macro user_account_rank_order() %}
has_no_source_id
, id_source_rank
, user_global_id desc
, sort_id desc nulls last
, id_source_user_id desc
{% endmacro %}

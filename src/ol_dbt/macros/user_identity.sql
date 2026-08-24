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
    The durable account identity: a source system's own primary key, which does not change.
    Rows with no source id (Emeritus and Global Alumni pre-date stable ids) fall back to
    email and are therefore NOT durable across an email edit. Nothing else identifies those
    accounts; see docs/design/adr_durable_user_surrogate_key.md.
#}
{% macro user_account_nk() %}
case
    when id_source_user_id is not null then id_source || ':' || id_source_user_id
    else 'email:' || email
end
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

{#--
    Table grants on StarRocks, for the per-model read access decided
    2026-11-24 (grants in dbt config, not per layer).

    dbt-starrocks 1.12.2 has none of these, so dbt-core's defaults run and all
    fail: `show grants on <relation>` is not StarRocks SQL, and a GRANT naming
    a table in an external catalog takes only a two-part name after
    SET CATALOG (a three-part name is a syntax error). Its table
    materialization never calls apply_grants at all; starrocks_table_grants
    below covers that from a project post-hook.

    A table grant on an external catalog is stored by name, so it outlives the
    table being dropped and re-created, and the current grants are always read
    and diffed. Only the roles in var('grant_roles')['starrocks'] are read or
    revoked: grants to any other role (service roles, admins) belong to
    whoever made them.

    The current grants come from SHOW GRANTS FOR ROLE, not sys.grants_to_roles:
    the view lists a database-wide grant once per table it covers, and a REVOKE
    ON TABLE of one of those fails ("There is no such grant defined on TABLE"),
    StarRocks 4.1.7.
--#}

{#-- The privileges each managed role holds on the relation itself, as
     {privilege: [role, ...]}. A row of SHOW GRANTS FOR ROLE reads e.g.
     "GRANT INSERT, SELECT ON TABLE db.t TO ROLE 'r'". --#}
{% macro starrocks_table_grants_held(relation, catalog) %}
    {% set on_table = " ON TABLE " ~ relation.schema ~ "." ~ relation.identifier ~ " TO ROLE " %}
    {% set held = {} %}
    {% for role in var("grant_roles")["starrocks"] %}
        {% for row in run_query("show grants for role " ~ role) %}
            {% if row["Catalog"] == catalog and on_table in row["Grants"] %}
                {% for privilege in row["Grants"].split(on_table)[0][6:].split(", ") %}
                    {% do held.setdefault(privilege | lower, []).append(role) %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {{ return(held) }}
{% endmacro %}

{%- macro starrocks__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(False) }}
{%- endmacro -%}

{%- macro starrocks__get_grant_sql(relation, privilege, grantees) -%}
    grant {{ privilege }} on table {{ relation.schema }}.{{ relation.identifier }} to role {{ grantees[0] }}
{%- endmacro -%}

{%- macro starrocks__get_revoke_sql(relation, privilege, grantees) -%}
    revoke {{ privilege }} on table {{ relation.schema }}.{{ relation.identifier }} from role {{ grantees[0] }}
{%- endmacro -%}

{#-- should_revoke is ignored: grants outlive re-creation, so they are always
     diffed (see above). A relation without a database is in the session's
     catalog, the profile's (as in starrocks_snapshot.sql). --#}
{% macro starrocks__apply_grants(relation, grant_config, should_revoke=True) %}
    {% if not grant_config %}
        {{ return(none) }}
    {% endif %}
    {% set catalog = relation.database or target.catalog %}
    {% set current_grants = starrocks_table_grants_held(relation, catalog) %}
    {% set statements = get_dcl_statement_list(
        relation, diff_of_two_dicts(current_grants, grant_config), get_revoke_sql
    ) + get_dcl_statement_list(
        relation, diff_of_two_dicts(grant_config, current_grants), get_grant_sql
    ) %}
    {% if not statements %}
        {{ log("On " ~ relation.render() ~ ": all grants are in place.") }}
        {{ return(none) }}
    {% endif %}
    {#-- One statement per call: the connection runs a single statement at a
         time. A relation in another catalog switches the session there and
         back to the profile's afterwards. --#}
    {% set switch_catalog = catalog != target.catalog %}
    {% if switch_catalog %}
        {% do run_query("set catalog " ~ catalog) %}
    {% endif %}
    {% for statement in statements %}
        {% do run_query(statement) %}
    {% endfor %}
    {% if switch_catalog %}
        {% do run_query("set catalog " ~ target.catalog) %}
    {% endif %}
{% endmacro %}

{#-- Project post-hook: grants for the table materialization, which in
     dbt-starrocks 1.12.2 never applies them. The incremental override
     (starrocks_incremental_materialization.sql) and dbt-core's snapshot
     materialization apply them themselves. --#}
{% macro starrocks_table_grants() %}
    {% if execute and target.type == "starrocks" and config.get("materialized") == "table" %}
        {% do apply_grants(this.incorporate(type="table"), config.get("grants"), should_revoke=True) %}
    {% endif %}
{% endmacro %}

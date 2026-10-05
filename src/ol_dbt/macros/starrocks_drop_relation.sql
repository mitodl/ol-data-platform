{#
    dbt-starrocks emits `drop table if exists <relation>` with no FORCE. In an
    Iceberg external catalog that removes the catalog entry and leaves every
    data and metadata file in S3: StarRocks passes the FORCE flag to Iceberg as
    the purge flag, so without it nothing is deleted. A table model drops its
    table on every run (on_table_exists defaults to 'replace'), an incremental
    model on --full-refresh, and every incremental run drops its temp relation,
    so each of those orphans a full file set.
    Upstream: https://github.com/StarRocks/dbt-starrocks/issues/124. Remove this
    override if that lands.

    The table and incremental materializations address an external relation
    with the catalog as its database. Everything else (e.g. seeds) leaves the
    database unset and resolves in the session's catalog, which is
    target.catalog. Internal-catalog tables keep the plain DROP, which sends
    them to the recycle bin where RECOVER TABLE can still reach them.
#}
{% macro starrocks__drop_relation(relation) -%}
  {%- set catalog = relation.database if relation.database is not none else target.catalog -%}
  {% call statement('drop_relation', auto_begin=False) %}
    {%- if relation.is_materialized_view -%}
        drop materialized view if exists {{ relation }};
    {%- elif relation.is_table and starrocks__external_catalog(catalog) is not none -%}
        drop table if exists {{ relation }} force;
    {%- else -%}
        drop {{ relation.type }} if exists {{ relation }};
    {%- endif -%}
  {% endcall %}
{%- endmacro %}

{#
    dbt-starrocks emits `drop table if exists <relation>` with no FORCE. In an
    Iceberg external catalog that removes the catalog entry and leaves every
    data and metadata file in S3: StarRocks passes the FORCE flag to Iceberg as
    the purge flag, so without it nothing is deleted. Every --full-refresh of a
    table or incremental model, and every incremental run's temp relation,
    orphans a full file set.
    Upstream: https://github.com/StarRocks/dbt-starrocks/issues/124. Remove this
    override if that lands.

    The adapter only sets a relation's database for an external catalog, so
    that is the test. Internal-catalog tables keep the plain DROP, which sends
    them to the recycle bin where RECOVER TABLE can still reach them.
#}
{% macro starrocks__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) %}
    {%- if relation.is_materialized_view -%}
        drop materialized view if exists {{ relation }};
    {%- elif relation.is_table and starrocks__external_catalog(relation.database) is not none -%}
        drop table if exists {{ relation }} force;
    {%- else -%}
        drop {{ relation.type }} if exists {{ relation }};
    {%- endif -%}
  {% endcall %}
{%- endmacro %}

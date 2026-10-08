{#
    dbt-starrocks ships no starrocks__create_schema, so dbt falls back to
    dbt-core's `create schema if not exists <schema>` with no PROPERTIES. In a
    Glue-backed Iceberg catalog that creates a database with no location, and
    every CTAS into it then fails with "Failed to find location in database".
    Upstream: https://github.com/StarRocks/dbt-starrocks/issues/123. Remove this
    override if that lands.

    The location is `<root>/<schema>`, where the root comes from the
    `iceberg_schema_location_roots` var keyed by catalog. Every suffixed schema
    dbt has created through Trino has that Glue location, so a
    developer-suffixed schema lands in the same place whichever engine made it.
    The canonical layer schemas (ol_warehouse_<env>_<layer>) sit at their own
    bucket roots, but those are provisioned outside dbt and never reach here:
    dbt only creates schemas that are missing.

    The adapter leaves a node's database unset and creates schemas in the
    session's catalog, so target.catalog is the catalog being written to. A
    model-level config(catalog=...) is not visible here and gets no schema.

    This does not cover the profile's own schema. If the adapter cannot connect
    to <catalog>.<schema> it runs a bare `CREATE DATABASE` for it from
    connections.py `open()`, before any macro runs, and this macro's
    `if not exists` then leaves that database without a location. For an
    external-catalog target the profile schema has to exist already, or be one
    no model is built into.
#}
{% macro starrocks__create_schema(relation) -%}
  {%- set catalog = target.catalog -%}
  {%- if catalog != 'default_catalog' -%}
    {%- set location_roots = var('iceberg_schema_location_roots') -%}
    {%- if catalog not in location_roots -%}
      {{ exceptions.raise_compiler_error(
        "No entry for catalog '" ~ catalog ~ "' in the iceberg_schema_location_roots var, "
        ~ "so schema '" ~ relation.schema ~ "' cannot be given a location."
      ) }}
    {%- endif -%}
  {%- endif -%}
  {% call statement('create_schema') %}
    create database if not exists {{ relation.without_identifier() }}
    {%- if catalog != 'default_catalog' %}
    properties ("location" = "{{ location_roots[catalog] }}/{{ relation.schema }}")
    {%- endif %}
  {% endcall %}
{%- endmacro %}

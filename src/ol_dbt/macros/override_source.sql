{% macro source(source_name, table_name) %}
  {#
    Override the default source() macro for DuckDB targets to route to Iceberg views

    For DuckDB: References glue__ prefixed views registered by register-glue-sources.py
    For Trino: Uses the built-in source() macro behavior
  #}
  {% if target.type == 'duckdb' %}
    {# Map source schemas to Glue database names #}
    {% set source_to_database_map = {
      'ol_warehouse_raw_data': 'ol_warehouse_production_raw',
      'ol_warehouse_staging': 'ol_warehouse_production_staging',
      'ol_warehouse_intermediate': 'ol_warehouse_production_intermediate',
      'ol_warehouse_dimensional': 'ol_warehouse_production_dimensional',
      'ol_warehouse_marts': 'ol_warehouse_production_mart',
      'ol_warehouse_reporting': 'ol_warehouse_production_reporting'
    } %}

    {% set glue_database = source_to_database_map.get(source_name, 'ol_warehouse_production_raw') %}

    {# Return the fully qualified view name #}
    glue__{{ glue_database }}__{{ table_name }}
  {% else %}
    {# For Trino, use the built-in builtins.source() macro #}
    {{ return(builtins.source(source_name, table_name)) }}
  {% endif %}
{% endmacro %}

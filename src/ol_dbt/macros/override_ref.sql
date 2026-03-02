{% macro ref(model_name) %}
  {#
    Override the default ref() macro for DuckDB dev_local target with smart fallback

    Strategy:
    1. Check if model exists locally (already built in this session)
    2. If yes: use local table
    3. If no: fall back to registered Glue view from production

    This allows incremental development - build what you're working on locally,
    automatically pull upstream dependencies from production.

    For Trino: Uses the built-in ref() macro behavior
  #}
  {% if target.type == 'duckdb' and target.name == 'dev_local' %}
    {# First, try to get the local relation using built-in ref() #}
    {% set local_relation = builtins.ref(model_name) %}

    {# Check if the local relation actually exists #}
    {% set relation_exists = adapter.get_relation(
      database=target.database,
      schema=local_relation.schema,
      identifier=local_relation.identifier
    ) %}

    {% if relation_exists %}
      {# Local table exists, use it #}
      {{ log("Using local table for ref('" ~ model_name ~ "'): " ~ local_relation, info=False) }}
      {{ return(local_relation) }}
    {% else %}
      {# Local table doesn't exist, try to fall back to Glue view #}
      {# Determine layer based on model name prefix #}
      {% if model_name.startswith('stg__') %}
        {% set glue_database = 'ol_warehouse_production_staging' %}
      {% elif model_name.startswith('int__') %}
        {% set glue_database = 'ol_warehouse_production_intermediate' %}
      {% elif model_name.startswith('dim__') %}
        {% set glue_database = 'ol_warehouse_production_dimensional' %}
      {% elif model_name.startswith('fct__') or model_name.startswith('marts__') %}
        {% set glue_database = 'ol_warehouse_production_mart' %}
      {% elif model_name.startswith('rpt__') %}
        {% set glue_database = 'ol_warehouse_production_reporting' %}
      {% else %}
        {% set glue_database = none %}
      {% endif %}

      {# If we found a matching layer, use the Glue view #}
      {% if glue_database %}
        {% set glue_view_name = 'glue__' ~ glue_database ~ '__' ~ model_name %}
        {{ log("Falling back to Glue view for ref('" ~ model_name ~ "'): " ~ glue_view_name, info=True) }}
        {{ glue_view_name }}
      {% else %}
        {# No Glue mapping found, return the local relation anyway (will fail if doesn't exist) #}
        {{ log("No Glue fallback available for ref('" ~ model_name ~ "'), using local reference", info=True) }}
        {{ return(local_relation) }}
      {% endif %}
    {% endif %}
  {% else %}
    {# For all non-DuckDB targets or other DuckDB targets, use built-in ref() #}
    {{ return(builtins.ref(model_name)) }}
  {% endif %}
{% endmacro %}

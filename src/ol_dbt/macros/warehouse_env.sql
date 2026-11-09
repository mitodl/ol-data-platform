{% macro warehouse_env() %}
  {#-
    The environment whose raw layer this build reads, e.g. `qa` for a target writing
    ol_warehouse_qa. The source definitions resolve their schema from target.schema the
    same way (`<target.schema without schema_suffix>_raw`), so this names the environment
    the rows came from. DuckDB targets read production's Glue tables (override_source),
    so they are `production`.
  -#}
  {%- if target.type == 'duckdb' -%}
    {{ return('production') }}
  {%- endif -%}
  {%- set prefix = 'ol_warehouse_' -%}
  {%- set base = target.schema.replace(var('schema_suffix', ''), '').rstrip('_') -%}
  {%- if not base.startswith(prefix) -%}
    {% do exceptions.raise_compiler_error(
      "No warehouse environment can be derived from target " ~ target.name ~ ": its schema '"
      ~ target.schema ~ "' does not start with " ~ prefix ~ "."
    ) %}
  {%- endif -%}
  {{ return(base[prefix | length:]) }}
{% endmacro %}

{% macro warehouse_var(name) %}
  {#-
    The value of the dbt var `name` (a mapping of environment to value) for the
    environment this build reads, e.g. the xPRO site the QA rows came from. Raises on an
    environment the mapping does not declare, so a new one cannot inherit production's
    URLs.

    Parsing renders every model under every target, including the default_catalog
    StarRocks targets whose schema names no environment, so parsing takes production's
    value. Only what the model refs is read from a parse.
  -#}
  {%- set values = var(name) -%}
  {%- if not execute -%}
    {{ return(values['production']) }}
  {%- endif -%}
  {%- set env = warehouse_env() -%}
  {%- if env not in values -%}
    {% do exceptions.raise_compiler_error(
      "dbt var " ~ name ~ " declares no value for warehouse environment '" ~ env
      ~ "' (declared: " ~ (values.keys() | sort | join(', ')) ~ ")."
    ) %}
  {%- endif -%}
  {{ return(values[env]) }}
{% endmacro %}

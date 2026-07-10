/*
 * Override for starrocks__olap_table and starrocks__get_create_materialized_view_as_sql
 * to guard against empty PROPERTIES dicts.
 *
 * The upstream adapter checks `properties is not none`, but an empty dict {}
 * (set via +properties: {} in dbt_project.yml to clear inherited Iceberg/Trino
 * properties) is not None and causes StarRocks to reject the DDL with
 * "No viable statement for input 'PROPERTIES ( )'". b2b_analytics's materialized
 * views (materialized='materialized_view') hit this via the second macro below;
 * its table models (materialized='table') hit it via the first.
 */

{% macro starrocks__olap_table(is_create_table_as) -%}

  {%- set is_create_table = is_create_table_as is none or not is_create_table_as -%}

  {%- set table_type = config.get('table_type', 'DUPLICATE') -%}
  {%- set keys = config.get('keys') -%}
  {%- set partition_by = config.get('partition_by') -%}
  {%- set partition_by_init = config.get('partition_by_init') -%}
  {%- set order_by = config.get('order_by') -%}
  {%- set partition_type = config.get('partition_type', 'RANGE') -%}
  {%- set buckets = config.get('buckets') -%}
  {%- set distributed_by = config.get('distributed_by') -%}
  {%- set properties = config.get('properties') -%}
  {%- set materialized = config.get('materialized', none) -%}
  {%- set unique_key = config.get('unique_key', none) -%}

  {%- if materialized == 'incremental' and unique_key is not none -%}
    {%- set table_type = 'PRIMARY' -%}
    {%- set keys = unique_key if unique_key is sequence and unique_key is not mapping and unique_key is not string else [unique_key] -%}
  {%- endif -%}

  {# 1. SET ENGINE #}
  {%- if is_create_table %} ENGINE = OLAP {% endif -%}

  {# 2. SET KEYS #}
  {%- if keys is not none -%}
    {%- if table_type == "DUPLICATE" %}
      DUPLICATE KEY (
        {%- for item in keys -%}
          {{ item }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
      )
    {%- elif table_type == "PRIMARY" %}
      PRIMARY KEY (
        {%- for item in keys -%}
          {{ item }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
      )
    {%- elif table_type == "UNIQUE" %}
      UNIQUE KEY (
        {%- for item in keys -%}
          {{ item }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
      )
    {%- else -%}
      {%- set msg -%}
        "{{ table_type }}" is not support
      {%- endset -%}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
  {%- else -%}
    {%- if table_type != "DUPLICATE" -%}
      {%- set msg -%}
        "{{ table_type }}" is must set "keys"
      {%- endset -%}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
  {% endif -%}

  {# 3. SET PARTITION #}
  {% if partition_by is not none -%}
    {{ starrocks__partition_by(partition_type, partition_by, partition_by_init) }}
  {%- endif %}

  {# 4. SET DISTRIBUTED #}
  {%- if distributed_by is not none %}
    DISTRIBUTED BY HASH (
      {%- for item in distributed_by -%}
        {{ item }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%}
    )
    {%- if buckets is not none -%}
      BUCKETS {{ buckets }}
    {%- elif adapter.is_before_version("2.5.7") -%}
      {%- set msg -%}
        [buckets] must set before version 2.5.7, current version is {{ adapter.current_version() }}
      {%- endset -%}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
  {%- elif adapter.is_before_version("3.1.0") -%}
    {%- set msg -%}
      [distributed_by] must set before version 3.1, current version is {{ adapter.current_version() }}
    {%- endset -%}
    {{ exceptions.raise_compiler_error(msg) }}
  {% endif -%}

  {# 5. SET ORDER BY #}
  {% if order_by is not none %}
    ORDER BY (
      {%- for item in order_by -%}
        {{ item }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%}
    )
  {% endif %}

  {# 6. SET PROPERTIES — skip if None or empty dict #}
  {% if properties is not none and properties | length > 0 %}
    PROPERTIES (
      {% for key, value in properties.items() -%}
        "{{ key }}" = "{{ value }}"
        {%- if not loop.last -%},
        {% endif -%}
      {%- endfor %}
    )
  {% endif %}
{%- endmacro %}

{% macro starrocks__get_create_materialized_view_as_sql(relation, sql) %}

    {%- set partition_by = config.get('partition_by') -%}
    {%- set buckets = config.get('buckets') -%}
    {%- set distributed_by = config.get('distributed_by') -%}
    {%- set properties = config.get('properties') -%}
    {%- set refresh_method = config.get('refresh_method', 'manual') -%}

    create materialized view {{ relation }}

    {%- if partition_by is not none -%}
        PARTITION BY (
        {%- for col in partition_by -%}
         {{ col }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
        )
    {%- endif -%}

    {%- if distributed_by is not none %}
    DISTRIBUTED BY HASH (
      {%- for item in distributed_by -%}
        {{ item }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%} )
      {%- if buckets is not none %}
        BUCKETS {{ buckets }}
      {% endif -%}
    {%- elif adapter.is_before_version("3.1.0") -%}
      {%- set msg -%}
        [distributed_by] must set before version 3.1, current version is {{ adapter.current_version() }}
      {%- endset -%}
      {{ exceptions.raise_compiler_error(msg) }}
    {% endif -%}
    refresh {{ refresh_method }}
    {% if properties is not none and properties | length > 0 %}
    PROPERTIES (
      {% for key, value in properties.items() %}
        "{{ key }}" = "{{ value }}"{% if not loop.last %},{% endif %}
      {% endfor %}
    )
    {% endif %}
    as
    {{ sql }};

{% endmacro %}

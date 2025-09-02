-- noqa: disable=LT05
{% macro generate_base_model_enhanced(
    source_name,
    table_name,
    leading_commas=False,
    case_sensitive_cols=False,
    materialized=None,
    entity_type=None,
    apply_transformations=True
) %}
    {{
        return(
            adapter.dispatch("generate_base_model_enhanced", "codegen")(
                source_name,
                table_name,
                leading_commas,
                case_sensitive_cols,
                materialized,
                entity_type,
                apply_transformations,
            )
        )
    }}
{% endmacro %}

{% macro default__generate_base_model_enhanced(
    source_name,
    table_name,
    leading_commas,
    case_sensitive_cols,
    materialized,
    entity_type,
    apply_transformations
) %}

    {%- set source_relation = source(source_name, table_name) -%}

    {# Try to get columns from the database relation first #}
    {%- set columns = adapter.get_columns_in_relation(source_relation) -%}
    {% set column_names = columns | map(attribute="name") | list %}

    {# If we got no columns from the database, try to get them from the graph/sources.yml #}
    {%- if column_names | length == 0 -%}
        {%- set source_node = (
            graph.sources.values()
            | selectattr("source_name", "equalto", source_name)
            | selectattr("name", "equalto", table_name)
            | first
        ) -%}
        {%- if source_node and source_node.columns -%}
            {%- set column_names = source_node.columns.keys() | list -%}
        {%- endif -%}
    {%- endif -%}

    {# Auto-detect entity type from table name if not provided #}
    {%- if entity_type is none -%}
        {%- if "user" in table_name -%}
            {%- set entity_type = "user" -%}
            {%- elif "course" in table_name and "run" in table_name -%}
            {%- set entity_type = "courserun" -%}
            {%- elif "course" in table_name -%}
            {%- set entity_type = "course" -%}
            {%- elif "video" in table_name -%}
            {%- set entity_type = "video" -%}
            {%- elif "program" in table_name and "run" in table_name -%}
            {%- set entity_type = "programrun" -%}
            {%- elif "program" in table_name -%}
            {%- set entity_type = "program" -%}
            {%- elif "website" in table_name -%}
            {%- set entity_type = "website" -%}
        {%- else -%} {%- set entity_type = "item" -%}
        {%- endif -%}
    {%- endif -%}

    {# Define column transformation rules #}
    {%- set timestamp_columns = [
        "created_on",
        "updated_on",
        "date_joined",
        "last_login",
        "start_date",
        "end_date",
        "enrollment_start",
        "enrollment_end",
        "expiration_date",
        "upgrade_deadline",
        "publish_date",
        "first_published_to_production",
        "created_at",
        "updated_at",
    ] -%}
    {%- set boolean_columns = [
        "is_active",
        "is_staff",
        "is_superuser",
        "live",
        "is_self_paced",
        "is_external",
        "multiangle",
        "is_logged_in_only",
        "is_public",
        "is_private",
    ] -%}

    {# Build column mappings #}
    {%- set column_mappings = {} -%}
    {%- if apply_transformations -%}
        {%- for column in column_names -%}
            {%- if column == "id" -%} {%- set new_name = entity_type ~ "_id" -%}
            {%- elif column == "title" -%} {%- set new_name = entity_type ~ "_title" -%}
            {%- elif column == "name" -%} {%- set new_name = entity_type ~ "_name" -%}
            {%- elif column == "username" -%} {%- set new_name = entity_type ~ "_username" -%}
            {%- elif column == "email" -%} {%- set new_name = entity_type ~ "_email" -%}
            {%- elif column == "description" -%} {%- set new_name = entity_type ~ "_description" -%}
            {%- elif column == "status" -%} {%- set new_name = entity_type ~ "_status" -%}
            {%- elif column == "live" -%} {%- set new_name = entity_type ~ "_is_live" -%}
            {%- elif column == "is_active" -%} {%- set new_name = entity_type ~ "_is_active" -%}
            {%- elif column in boolean_columns -%} {%- set new_name = entity_type ~ "_" ~ column -%}
            {%- elif column == "created_on" -%} {%- set new_name = entity_type ~ "_created_on" -%}
            {%- elif column == "updated_on" -%} {%- set new_name = entity_type ~ "_updated_on" -%}
            {%- elif column == "date_joined" -%} {%- set new_name = entity_type ~ "_joined_on" -%}
            {%- elif column == "last_login" -%} {%- set new_name = entity_type ~ "_last_login" -%}
            {%- elif column == "start_date" -%} {%- set new_name = entity_type ~ "_start_on" -%}
            {%- elif column == "end_date" -%} {%- set new_name = entity_type ~ "_end_on" -%}
            {%- elif column == "created_at" -%} {%- set new_name = entity_type ~ "_created_on" -%}
            {%- elif column == "updated_at" -%} {%- set new_name = entity_type ~ "_updated_on" -%}
            {%- else -%} {%- set new_name = column -%}
            {%- endif -%}
            {%- set _ = column_mappings.update({column: new_name}) -%}
        {%- endfor -%}
    {%- else -%}
        {%- for column in column_names -%} {%- set _ = column_mappings.update({column: column}) -%} {%- endfor -%}
    {%- endif -%}

    {# Check if table needs deduplication (has airbyte metadata) #}
    {%- set needs_deduplication = "id" in column_names and "_airbyte_extracted_at" in column_names -%}

    {% set base_model_sql %}
{%- if materialized is not none -%}
{{ "{{ config(materialized='" ~ materialized ~ "') }}" }}
{%- endif -%}

{# Add comment based on entity type #}
{%- if entity_type == 'user' -%}
-- User Information
{%- elif entity_type == 'course' -%}
-- Course Information
{%- elif entity_type == 'courserun' -%}
-- Course Run Information
{%- elif entity_type == 'video' -%}
-- Video Information
{%- elif entity_type == 'website' -%}
-- Website Information
{%- else -%}
-- {{ table_name | replace('_', ' ') | title }} Information
{%- endif %}

with source as (
    select * from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ table_name }}'{% raw %}) }}{% endraw %}
)

{%- if needs_deduplication %}

{{ "{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}" }}
{%- endif %}

{# Build non-airbyte columns list #}
{%- set non_airbyte_columns = [] -%}
{%- for column in column_names -%}
{%- if not column.startswith('_airbyte') -%}
{%- set _ = non_airbyte_columns.append(column) -%}
{%- endif -%}
{%- endfor -%}

, cleaned as (
    select
{%- for column in non_airbyte_columns %}
        {% if column in timestamp_columns and apply_transformations -%}
{{ "{{ cast_timestamp_to_iso8601('" ~ column ~ "') }}" }} as {{ column_mappings[column] }}
        {%- elif column == 'name' and entity_type == 'user' and apply_transformations -%}
replace(replace(replace({{ column }}, ' ', '<>'), '><', ''), '<>', ' ') as {{ column_mappings[column] }}
        {%- elif column in boolean_columns and 'edxorg' in table_name and apply_transformations -%}
cast({{ column }} as boolean) as {{ column_mappings[column] }}
        {%- else -%}
{% if not case_sensitive_cols %}{{ column | lower }}{% else %}{{ adapter.quote(column) }}{% endif %} as {{ column_mappings[column] }}
        {%- endif -%}
{%- if not loop.last -%}
        ,{%- endif %}
{%- endfor %}
    from {% if needs_deduplication %}most_recent_source{% else %}source{% endif %}
)

select
{%- for column in non_airbyte_columns %}
    {{ column_mappings[column] }}{{ "," if not loop.last }}
{%- endfor %}
from cleaned
    {% endset %}
    {% if execute %} {{ print(base_model_sql) }} {% do return(base_model_sql) %} {% endif %}
{% endmacro %}

-- Enhanced version of generate_model_yaml that works with our enhanced staging models
-- This macro compiles models first to get their actual column structure
{% macro generate_model_yaml_enhanced(model_names=[], upstream_descriptions=False, include_data_types=True) -%}
    {{
        return(
            adapter.dispatch("generate_model_yaml_enhanced", "codegen")(
                model_names, upstream_descriptions, include_data_types
            )
        )
    }}
{%- endmacro %}

{% macro default__generate_model_yaml_enhanced(model_names, upstream_descriptions, include_data_types) %}

    {% set model_yaml = [] %}

    {% do model_yaml.append("version: 2") %}
    {% do model_yaml.append("") %}
    {% do model_yaml.append("models:") %}

    {% if model_names is string %}
        {{
            exceptions.raise_compiler_error(
                "The `model_names` argument must always be a list, even if there is only one model."
            )
        }}
    {% else %}
        {% for model in model_names %}
            {% do model_yaml.append("  - name: " ~ model | lower) %}
            {% do model_yaml.append('    description: ""') %}
            {% do model_yaml.append("    columns:") %}

            {% set columns = [] %}
            {% set column_desc_dict = {} %}

            {% if execute %}
                {# Find the model in the graph and get columns from final SELECT #}
                {% set columns = get_model_columns_from_graph(model) %}
            {% endif %}

            {# Get upstream descriptions if requested #}
            {% if upstream_descriptions %}
                {% set column_desc_dict = codegen.build_dict_column_descriptions(model) %}
            {% endif %}

            {# Generate column YAML manually since we have simple column info #}
            {% for column in columns %}
                {% do model_yaml.append("      - name: " ~ column.name | lower) %}
                {% if include_data_types and column.data_type %}
                    {% do model_yaml.append("        data_type: " ~ column.data_type) %}
                {% endif %}
                {% set desc = column_desc_dict.get(column.name | lower, "") %}
                {% do model_yaml.append('        description: "' ~ desc ~ '"') %}
                {% do model_yaml.append("") %}
            {% endfor %}
        {% endfor %}
    {% endif %}

    {% if execute %} {% set joined = model_yaml | join("\n") %} {{ print(joined) }} {% do return(joined) %} {% endif %}

{% endmacro %}

{# Get columns by parsing the final SELECT from the model and enriching with source data types #}
{% macro get_model_columns_from_graph(model_name) %}
    {% set ns = namespace(columns=[]) %}

    {% if execute %}
        {# Find the model in the graph #}
        {% for node_id, node in graph.nodes.items() %}
            {% if node.name == model_name %}
                {# Get the compiled SQL or raw SQL #}
                {% set sql = node.compiled_code if node.compiled_code else node.raw_code %}

                {% if sql %}
                    {# Parse the final SELECT statement to extract column names #}
                    {% set column_names = parse_final_select_statement(sql) %}

                    {# Get source table information to map data types #}
                    {% set source_table_name = extract_source_table_from_sql(sql) %}
                    {% set source_columns_map = get_source_columns_map(source_table_name) %}

                    {# Create column objects with proper data types #}
                    {% for col_name in column_names %}
                        {% set source_col_info = get_source_column_type(col_name, source_columns_map, sql) %}
                        {% set col_obj = {"name": col_name, "data_type": source_col_info.data_type} %}
                        {% do ns.columns.append(col_obj) %}
                    {% endfor %}
                {% endif %}

                {% break %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% do return(ns.columns) %}
{% endmacro %}

{# Extract the source table name from the SQL #}
{% macro extract_source_table_from_sql(sql) %}
    {# Look for source('source_name', 'table_name') pattern #}
    {% set source_pattern = "source\\s*\\(\\s*['\"]([^'\"]+)['\"]\\s*,\\s*['\"]([^'\"]+)['\"]\\s*\\)" %}
    {% set matches = modules.re.findall(source_pattern, sql) %}
    {% if matches and matches | length > 0 %}
        {% do return(matches[0][1]) %}  {# Return the table name #}
    {% endif %}
    {% do return("") %}
{% endmacro %}

{# Get source columns mapping from the graph #}
{% macro get_source_columns_map(source_table_name) %}
    {% set ns = namespace(columns_map={}) %}

    {% if execute and source_table_name %}
        {# Find the source in the graph #}
        {% for source_id, source_node in graph.sources.items() %}
            {% if source_node.name == source_table_name %}
                {% for column in source_node.columns.values() %}
                    {% do ns.columns_map.update({column.name: column}) %}
                {% endfor %}
                {% break %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% do return(ns.columns_map) %}
{% endmacro %}

{# Get the data type for a column by mapping it back to source #}
{% macro get_source_column_type(staging_col_name, source_columns_map, sql) %}
    {% set ns = namespace(data_type="text") %}

    {# Try to find the source column mapping by looking at the SQL transformations #}
    {% set source_col_name = map_staging_to_source_column(staging_col_name, sql) %}

    {% if source_col_name and source_col_name in source_columns_map %}
        {% set source_column = source_columns_map[source_col_name] %} {% set ns.data_type = source_column.data_type %}
    {% endif %}

    {% do return({"data_type": ns.data_type}) %}
{% endmacro %}

{# Map staging column names back to source column names by parsing the SELECT clause #}
{% macro map_staging_to_source_column(staging_col_name, sql) %}
    {# Look for patterns like:
       - "source_col as staging_col"
       - "function(source_col) as staging_col"
       - "source_col," (same name)
    #}
    {# Extract the cleaned CTE part of the SQL #}
    {% set cleaned_start = sql.lower().find(", cleaned as (") %}
    {% set cleaned_end = sql.lower().find(")", cleaned_start) %}

    {% if cleaned_start >= 0 and cleaned_end >= 0 %}
        {% set cleaned_sql = sql[cleaned_start:cleaned_end] %}

        {# Look for the staging column in the cleaned SELECT #}
        {% set lines = cleaned_sql.split("\n") %}
        {% for line in lines %}
            {% set clean_line = line.strip().rstrip(",") %}
            {% if " as " ~ staging_col_name in clean_line.lower() %}
                {# Extract the source column name before 'as' #}
                {% set as_pos = clean_line.lower().find(" as " ~ staging_col_name.lower()) %}
                {% set source_part = clean_line[:as_pos].strip() %}

                {# Handle different patterns #}
                {% if source_part.startswith("{{") %}
                    {# Macro call like {{ cast_timestamp_to_iso8601('last_login') }} #}
                    {% set macro_match = modules.re.search("\\('([^']+)'\\)", source_part) %}
                    {% if macro_match %} {% do return(macro_match.group(1)) %} {% endif %}
                {% else %}
                    {# Simple column reference #}
                    {% do return(source_part) %}
                {% endif %}
            {% elif clean_line.lower() == staging_col_name.lower() %}
                {# Same name, no 'as' clause #}
                {% do return(staging_col_name) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {# Fallback: try common name mappings #}
    {% if staging_col_name.startswith("user_") %} {% do return(staging_col_name[5:]) %}  {# Remove user_ prefix #}
    {% elif staging_col_name.startswith("item_") %} {% do return(staging_col_name[5:]) %}  {# Remove item_ prefix #}
    {% endif %}

    {% do return(staging_col_name) %}
{% endmacro %}

{# Parse final SELECT statement to extract column names #}
{% macro parse_final_select_statement(sql) %}
    {% set ns = namespace(columns=[]) %}

    {% if sql %}
        {# Find the last occurrence of 'select' before 'from cleaned' #}
        {% set select_pos = sql.lower().rfind("select") %}
        {% set from_cleaned_pos = sql.lower().find("from cleaned") %}

        {% if select_pos >= 0 and from_cleaned_pos >= 0 and select_pos < from_cleaned_pos %}
            {# Extract the portion between select and from cleaned #}
            {% set select_part = sql[select_pos:from_cleaned_pos] %}

            {# Split by lines and extract column names #}
            {% set lines = select_part.split("\n") %}
            {% for line in lines %}
                {% set clean_line = line.strip() %}
                {% if clean_line and not clean_line.lower().startswith("select") and not clean_line.startswith(
                    "--"
                ) %}
                    {% set col_clean = clean_line.rstrip(",").strip() %}
                    {% if col_clean %} {% do ns.columns.append(col_clean) %} {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}

    {% do return(ns.columns) %}
{% endmacro %}

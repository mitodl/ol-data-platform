{% macro starrocks__create_csv_table(model, agate_table) -%}
    {#
        dbt-starrocks 1.12's create_csv_table, except that a seed in an external
        catalog gets a plain column list. The adapter always adds its native table
        clause, which StarRocks rejects there ("Can't create OLAP table in the
        iceberg catalog"), and every lake target writes to a Glue Iceberg catalog.
        The seed's relation carries no catalog, so the catalog is the model's
        config(catalog=...) or the target's, which the session is using.
    #}
    {%- set column_override = model['config'].get('column_types', {}) -%}
    {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
    {%- set engine = config.get('engine', 'OLAP') -%}
    {% set sql %}
        create table {{ this.render() }} (
            {%- for col_name in agate_table.column_names %}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) %}
            {{ adapter.quote_seed_column(col_name | string, quote_seed_column) }} {{ column_override.get(col_name, inferred_type) }}
            {%- if not loop.last %},{% endif %}
            {%- endfor %}
        )
        {%- if starrocks__is_internal_catalog() %}
            {%- if engine == 'OLAP' %}
        {{ starrocks__olap_table(False) }}
            {%- else %}
        {{ starrocks__other_table() }}
            {%- endif %}
        {%- endif %}
    {% endset %}
    {% call statement('_') -%}
        {{ sql }}
    {%- endcall %}
    {{ return(sql) }}
{%- endmacro %}
